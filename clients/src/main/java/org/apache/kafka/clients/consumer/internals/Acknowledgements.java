/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.common.message.ShareAcknowledgeRequestData;
import org.apache.kafka.common.message.ShareFetchRequestData;
import org.apache.kafka.common.protocol.Errors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * This class maintains the acknowledgement and gap information for a set of records on a single
 * topic-partition being delivered to a consumer in a share group.
 */
public class Acknowledgements {
    public static final byte ACKNOWLEDGE_TYPE_GAP = (byte) 0;
    public static final int MAX_RECORDS_WITH_SAME_ACKNOWLEDGE_TYPE = 10;

    // The acknowledgements keyed by offset. If the record is a gap, the AcknowledgeType will be null.
    private final Map<Long, AcknowledgeType> acknowledgements;

    // When the broker responds to the acknowledgements, this is the error code returned.
    private Errors acknowledgeErrorCode;

    public static Acknowledgements empty() {
        return new Acknowledgements(new TreeMap<>());
    }

    private Acknowledgements(Map<Long, AcknowledgeType> acknowledgements) {
        this.acknowledgements = acknowledgements;
    }

    /**
     * Adds an acknowledgement for a specific offset. Will overwrite an existing
     * acknowledgement for the same offset.
     *
     * @param offset The record offset.
     * @param type   The AcknowledgeType.
     */
    public void add(long offset, AcknowledgeType type) {
        this.acknowledgements.put(offset, type);
    }

    /**
     * Adds an acknowledgement for a specific offset. Will <b>not</b> overwrite an existing
     * acknowledgement for the same offset.
     *
     * @param offset The record offset.
     * @param type   The AcknowledgeType.
     */
    public void addIfAbsent(long offset, AcknowledgeType type) {
        acknowledgements.putIfAbsent(offset, type);
    }

    /**
     * Adds a gap for the specified offset. This means the broker expected a record at this offset
     * but when the record batch was parsed, the desired offset was missing. This occurs when the
     * record has been removed by the log compactor.
     *
     * @param offset The record offset.
     */
    public void addGap(long offset) {
        acknowledgements.put(offset, null);
    }

    /**
     * Gets the acknowledgement type for an offset.
     *
     * @param offset The record offset.
     * @return The AcknowledgeType, or null if no acknowledgement is present.
     */
    public AcknowledgeType get(long offset) {
        return acknowledgements.get(offset);
    }

    /**
     * Whether the set of acknowledgements is empty.
     *
     * @return Whether the set of acknowledgements is empty.
     */
    public boolean isEmpty() {
        return acknowledgements.isEmpty();
    }

    /**
     * Returns the size of the set of acknowledgements.
     *
     * @return The size of the set of acknowledgements.
     */
    public int size() {
        return acknowledgements.size();
    }

    /**
     * Whether the acknowledgements were sent to the broker and a response received.
     *
     * @return Whether the acknowledgements were sent to the broker and a response received
     */
    public boolean isCompleted() {
        return acknowledgeErrorCode != null;
    }

    /**
     * Set the acknowledgement error code when the response has been received from the broker.
     *
     * @param acknowledgeErrorCode the error code
     */
    public void setAcknowledgeErrorCode(Errors acknowledgeErrorCode) {
        this.acknowledgeErrorCode = acknowledgeErrorCode;
    }

    /**
     * Get the acknowledgement error code when the response has been received from the broker.
     *
     * @return the error code
     */
    public Errors getAcknowledgeErrorCode() {
        return acknowledgeErrorCode;
    }

    /**
     * Merges two sets of acknowledgements. If there are overlapping acknowledgements, the
     * merged set wins.
     *
     * @param other The set of acknowledgement to merge.
     */
    public Acknowledgements merge(Acknowledgements other) {
        acknowledgements.putAll(other.acknowledgements);
        return this;
    }

    /**
     * Returns the Map of Acknowledgements for the offsets.
     */
    public Map<Long, AcknowledgeType> getAcknowledgementsTypeMap() {
        return acknowledgements;
    }

    public List<ShareFetchRequestData.AcknowledgementBatch> getShareFetchBatches() {
        List<ShareFetchRequestData.AcknowledgementBatch> batches = new ArrayList<>();
        if (acknowledgements.isEmpty()) return batches;
        ShareFetchRequestData.AcknowledgementBatch currentBatch = null;
        for (Map.Entry<Long, AcknowledgeType> entry : acknowledgements.entrySet()) {
            if (currentBatch == null) {
                currentBatch = new ShareFetchRequestData.AcknowledgementBatch();
                currentBatch.setFirstOffset(entry.getKey());
                currentBatch.setLastOffset(entry.getKey());
                if (entry.getValue() != null) {
                    currentBatch.acknowledgeTypes().add(entry.getValue().id);
                } else {
                    currentBatch.acknowledgeTypes().add(ACKNOWLEDGE_TYPE_GAP);
                }
            } else {
                if (entry.getValue() != null) {
                    if (entry.getKey() == currentBatch.lastOffset() + 1) {
                        currentBatch.setLastOffset(entry.getKey());
                        currentBatch.acknowledgeTypes().add(entry.getValue().id);
                    } else {
                        List<ShareFetchRequestData.AcknowledgementBatch> optimalBatches = maybeOptimiseAcknowledgementTypesForFetch(currentBatch);

                        optimalBatches.forEach(batch -> {
                            if (canOptimiseForSingleAcknowledgeType(batch)) {
                                // If the batch had a single acknowledgement type, we optimise the array further independent
                                // of the number of records.
                                batch.acknowledgeTypes().subList(1, batch.acknowledgeTypes().size()).clear();
                            }
                            batches.add(batch);
                        });

                        currentBatch = new ShareFetchRequestData.AcknowledgementBatch();
                        currentBatch.setFirstOffset(entry.getKey());
                        currentBatch.setLastOffset(entry.getKey());
                        currentBatch.acknowledgeTypes().add(entry.getValue().id);
                    }
                } else {
                    if (entry.getKey() == currentBatch.lastOffset() + 1) {
                        currentBatch.setLastOffset(entry.getKey());
                        currentBatch.acknowledgeTypes().add(ACKNOWLEDGE_TYPE_GAP);
                    } else {
                        List<ShareFetchRequestData.AcknowledgementBatch> optimalBatches = maybeOptimiseAcknowledgementTypesForFetch(currentBatch);

                        optimalBatches.forEach(batch -> {
                            if (canOptimiseForSingleAcknowledgeType(batch)) {
                                // If the batch had a single acknowledgement type, we optimise the array independent
                                // of the number of records.
                                batch.acknowledgeTypes().subList(1, batch.acknowledgeTypes().size()).clear();
                            }
                            batches.add(batch);
                        });

                        currentBatch = new ShareFetchRequestData.AcknowledgementBatch();
                        currentBatch.setFirstOffset(entry.getKey());
                        currentBatch.setLastOffset(entry.getKey());
                        currentBatch.acknowledgeTypes().add(ACKNOWLEDGE_TYPE_GAP);
                    }
                }
            }
        }
        List<ShareFetchRequestData.AcknowledgementBatch> optimalBatches = maybeOptimiseAcknowledgementTypesForFetch(currentBatch);

        optimalBatches.forEach(batch -> {
            if (canOptimiseForSingleAcknowledgeType(batch)) {
                // If the batch had a single acknowledgement type, we optimise the array independent
                // of the number of records.
                batch.acknowledgeTypes().subList(1, batch.acknowledgeTypes().size()).clear();
            }
            batches.add(batch);
        });
        return batches;
    }

    public List<ShareAcknowledgeRequestData.AcknowledgementBatch> getShareAcknowledgeBatches() {
        List<ShareAcknowledgeRequestData.AcknowledgementBatch> batches = new ArrayList<>();
        if (acknowledgements.isEmpty()) return batches;
        ShareAcknowledgeRequestData.AcknowledgementBatch currentBatch = null;
        for (Map.Entry<Long, AcknowledgeType> entry : acknowledgements.entrySet()) {
            if (currentBatch == null) {
                currentBatch = new ShareAcknowledgeRequestData.AcknowledgementBatch();
                currentBatch.setFirstOffset(entry.getKey());
                currentBatch.setLastOffset(entry.getKey());
                if (entry.getValue() != null) {
                    currentBatch.acknowledgeTypes().add(entry.getValue().id);
                } else {
                    currentBatch.acknowledgeTypes().add(ACKNOWLEDGE_TYPE_GAP);
                }
            } else {
                if (entry.getValue() != null) {
                    if (entry.getKey() == currentBatch.lastOffset() + 1) {
                        currentBatch.setLastOffset(entry.getKey());
                        currentBatch.acknowledgeTypes().add(entry.getValue().id);
                    } else {
                        List<ShareAcknowledgeRequestData.AcknowledgementBatch> optimalBatches = maybeOptimiseAcknowledgementTypesForAcknowledge(currentBatch);

                        optimalBatches.forEach(batch -> {
                            if (canOptimiseForSingleAcknowledgeType(batch)) {
                                // If the batch had a single acknowledgement type, we optimise the array independent
                                // of the number of records.
                                batch.acknowledgeTypes().subList(1, batch.acknowledgeTypes().size()).clear();
                            }
                            batches.add(batch);
                        });

                        currentBatch = new ShareAcknowledgeRequestData.AcknowledgementBatch();
                        currentBatch.setFirstOffset(entry.getKey());
                        currentBatch.setLastOffset(entry.getKey());
                        currentBatch.acknowledgeTypes().add(entry.getValue().id);
                    }
                } else {
                    if (entry.getKey() == currentBatch.lastOffset() + 1) {
                        currentBatch.setLastOffset(entry.getKey());
                        currentBatch.acknowledgeTypes().add(ACKNOWLEDGE_TYPE_GAP);
                    } else {
                        List<ShareAcknowledgeRequestData.AcknowledgementBatch> optimalBatches = maybeOptimiseAcknowledgementTypesForAcknowledge(currentBatch);

                        optimalBatches.forEach(batch -> {
                            if (canOptimiseForSingleAcknowledgeType(batch)) {
                                // If the batch had a single acknowledgement type, we optimise the array independent
                                // of the number of records.
                                batch.acknowledgeTypes().subList(1, batch.acknowledgeTypes().size()).clear();
                            }
                            batches.add(batch);
                        });

                        currentBatch = new ShareAcknowledgeRequestData.AcknowledgementBatch();
                        currentBatch.setFirstOffset(entry.getKey());
                        currentBatch.setLastOffset(entry.getKey());
                        currentBatch.acknowledgeTypes().add(ACKNOWLEDGE_TYPE_GAP);
                    }
                }
            }
        }
        List<ShareAcknowledgeRequestData.AcknowledgementBatch> optimalBatches = maybeOptimiseAcknowledgementTypesForAcknowledge(currentBatch);

        optimalBatches.forEach(batch -> {
            if (canOptimiseForSingleAcknowledgeType(batch)) {
                // If the batch had a single acknowledgement type, we optimise the array independent
                // of the number of records.
                batch.acknowledgeTypes().subList(1, batch.acknowledgeTypes().size()).clear();
            }
            batches.add(batch);
        });
        return batches;
    }

    /**
     * Traverses the shareFetchBatch and splits it into optimal batches wherever possible.
     * Optimisation happens when a batch has continuous records of the same acknowledge type
     * whose count exceeds the default value. In this case, the batch is split into 2 such that the
     * batch with the continuous records has only 1 acknowledge type in its array.
     */
    private List<ShareFetchRequestData.AcknowledgementBatch> maybeOptimiseAcknowledgementTypesForFetch(ShareFetchRequestData.AcknowledgementBatch currentShareFetchBatch) {
        List<ShareFetchRequestData.AcknowledgementBatch> batches = new ArrayList<>();
        if (currentShareFetchBatch == null) return batches;

        long currentOffset = currentShareFetchBatch.firstOffset();
        int currentStartIndex = 0;
        int recordsWithSameAcknowledgeType = 1;
        for (int i = 1; i < currentShareFetchBatch.acknowledgeTypes().size(); i++) {
            byte acknowledgeType = currentShareFetchBatch.acknowledgeTypes().get(i);

            // If we have a continuous set of records with the same acknowledgement type exceeding the default count,
            // then we optimise the batches to include only start and end offset and have only 1 acknowledge type in the array.
            byte prevAcknowledgeType = currentShareFetchBatch.acknowledgeTypes().get(i - 1);
            if (acknowledgeType == prevAcknowledgeType && recordsWithSameAcknowledgeType >= MAX_RECORDS_WITH_SAME_ACKNOWLEDGE_TYPE) {
                // We continue traversing until we have the same acknowledge type.
                while (i < currentShareFetchBatch.acknowledgeTypes().size()) {
                    byte acknowledgeType2 = currentShareFetchBatch.acknowledgeTypes().get(i);

                    if (acknowledgeType2 != currentShareFetchBatch.acknowledgeTypes().get(i - 1)) break;
                    i++;
                    recordsWithSameAcknowledgeType++;
                }

                // Now we prepare 2 batches, one starting just before the batch with single acknowledge type
                // and one with the single acknowledge type.
                ShareFetchRequestData.AcknowledgementBatch batch1 = new ShareFetchRequestData.AcknowledgementBatch();
                batch1.setFirstOffset(currentOffset);
                batch1.setLastOffset(currentOffset + i - recordsWithSameAcknowledgeType - currentStartIndex - 1);
                if (batch1.lastOffset() >= batch1.firstOffset()) {
                    batch1.setAcknowledgeTypes(new ArrayList<>(currentShareFetchBatch.acknowledgeTypes().subList(currentStartIndex,
                            i - recordsWithSameAcknowledgeType)));
                    batches.add(batch1);
                }

                ShareFetchRequestData.AcknowledgementBatch batch2 = new ShareFetchRequestData.AcknowledgementBatch();
                batch2.setFirstOffset(currentOffset + i - recordsWithSameAcknowledgeType - currentStartIndex);
                batch2.setLastOffset(currentOffset + i - currentStartIndex - 1);
                batch2.acknowledgeTypes().add(acknowledgeType);

                batches.add(batch2);
                recordsWithSameAcknowledgeType = 1;

                // Updating the offset and startIndex for further iterations.
                currentOffset = currentOffset + i - currentStartIndex;
                currentStartIndex = i;
            } else if (acknowledgeType == prevAcknowledgeType) {
                // The maximum limit has not yet been reached, we increment the count and move ahead.
                recordsWithSameAcknowledgeType++;
            } else {
                recordsWithSameAcknowledgeType = 1;
            }
        }
        if (currentStartIndex < currentShareFetchBatch.acknowledgeTypes().size()) {
            ShareFetchRequestData.AcknowledgementBatch batch = new ShareFetchRequestData.AcknowledgementBatch();
            batch.setFirstOffset(currentOffset);
            batch.setLastOffset(currentOffset + currentShareFetchBatch.acknowledgeTypes().size() - currentStartIndex - 1);
            batch.setAcknowledgeTypes(new ArrayList<>(currentShareFetchBatch.acknowledgeTypes().subList(currentStartIndex,
                    currentShareFetchBatch.acknowledgeTypes().size())));
            batches.add(batch);
        }
        return batches;
    }

    /**
     * Traverses the shareAcknowledgeBatch and splits it into optimal batches wherever possible.
     * Optimisation happens when a batch has continuous records of the same acknowledge type
     * whose count exceeds the default value. In this case, the batch is split into 2 such that the
     * batch with the continuous records has only 1 acknowledge type in its array.
     */
    private List<ShareAcknowledgeRequestData.AcknowledgementBatch> maybeOptimiseAcknowledgementTypesForAcknowledge(ShareAcknowledgeRequestData.AcknowledgementBatch currentShareAcknowledgeBatch) {
        List<ShareAcknowledgeRequestData.AcknowledgementBatch> batches = new ArrayList<>();
        if (currentShareAcknowledgeBatch == null) return batches;

        long currentOffset = currentShareAcknowledgeBatch.firstOffset();
        int currentStartIndex = 0;
        int recordsWithSameAcknowledgeType = 1;
        for (int i = 1; i < currentShareAcknowledgeBatch.acknowledgeTypes().size(); i++) {
            byte acknowledgeType = currentShareAcknowledgeBatch.acknowledgeTypes().get(i);
            // If we have a continuous set of records with the same acknowledgement type exceeding the default count,
            // then we optimise the batches to include only start and end offset and have only 1 acknowledge type in the array.
            byte prevAcknowledgeType = currentShareAcknowledgeBatch.acknowledgeTypes().get(i - 1);
            if (acknowledgeType == prevAcknowledgeType && recordsWithSameAcknowledgeType >= MAX_RECORDS_WITH_SAME_ACKNOWLEDGE_TYPE) {
                // We continue traversing until we have the same acknowledge type.
                while (i < currentShareAcknowledgeBatch.acknowledgeTypes().size()) {
                    byte acknowledgeType2 = currentShareAcknowledgeBatch.acknowledgeTypes().get(i);

                    if (acknowledgeType2 != currentShareAcknowledgeBatch.acknowledgeTypes().get(i - 1)) break;
                    i++;
                    recordsWithSameAcknowledgeType++;
                }

                // Now we prepare 2 batches, one starting just before the batch with single acknowledge type
                // and one with the single acknowledge type.
                ShareAcknowledgeRequestData.AcknowledgementBatch batch1 = new ShareAcknowledgeRequestData.AcknowledgementBatch();
                batch1.setFirstOffset(currentOffset);
                batch1.setLastOffset(currentOffset + i - recordsWithSameAcknowledgeType - currentStartIndex - 1);
                if (batch1.lastOffset() >= batch1.firstOffset()) {
                    batch1.setAcknowledgeTypes(new ArrayList<>(currentShareAcknowledgeBatch.acknowledgeTypes().subList(currentStartIndex,
                            i - recordsWithSameAcknowledgeType)));
                    batches.add(batch1);
                }

                ShareAcknowledgeRequestData.AcknowledgementBatch batch2 = new ShareAcknowledgeRequestData.AcknowledgementBatch();
                batch2.setFirstOffset(currentOffset + i - recordsWithSameAcknowledgeType - currentStartIndex);
                batch2.setLastOffset(currentOffset + i - currentStartIndex - 1);
                batch2.acknowledgeTypes().add(acknowledgeType);

                batches.add(batch2);
                recordsWithSameAcknowledgeType = 1;

                // Updating the offset and startIndex for further iterations.
                currentOffset = currentOffset + i - currentStartIndex;
                currentStartIndex = i;
            } else if (acknowledgeType == prevAcknowledgeType) {
                // The maximum limit has not yet been reached, we increment the count and move ahead.
                recordsWithSameAcknowledgeType++;
            } else {
                recordsWithSameAcknowledgeType = 1;
            }
        }
        if (currentStartIndex < currentShareAcknowledgeBatch.acknowledgeTypes().size()) {
            ShareAcknowledgeRequestData.AcknowledgementBatch batch = new ShareAcknowledgeRequestData.AcknowledgementBatch();
            batch.setFirstOffset(currentOffset);
            batch.setLastOffset(currentOffset + currentShareAcknowledgeBatch.acknowledgeTypes().size() - currentStartIndex - 1);
            batch.setAcknowledgeTypes(new ArrayList<>(currentShareAcknowledgeBatch.acknowledgeTypes().subList(currentStartIndex,
                    currentShareAcknowledgeBatch.acknowledgeTypes().size())));
            batches.add(batch);
        }
        return batches;
    }

    /**
     * @return Returns true if the array of acknowledge types in the share fetch batch contains a single acknowledge type
     * and the array size can be reduced to 1.
     * Returns false when the array has more than one acknowledge type or is already optimised.
     */
    private boolean canOptimiseForSingleAcknowledgeType(ShareFetchRequestData.AcknowledgementBatch shareFetchBatch) {
        if (shareFetchBatch == null || shareFetchBatch.acknowledgeTypes().size() == 1) return false;
        int firstAcknowledgeType = shareFetchBatch.acknowledgeTypes().get(0);
        for (int i = 1; i < shareFetchBatch.acknowledgeTypes().size(); i++) {
            if (shareFetchBatch.acknowledgeTypes().get(i) != firstAcknowledgeType) return false;
        }
        return true;
    }

    private boolean canOptimiseForSingleAcknowledgeType(ShareAcknowledgeRequestData.AcknowledgementBatch shareAcknowledgeBatch) {
        if (shareAcknowledgeBatch == null || shareAcknowledgeBatch.acknowledgeTypes().size() == 1) return false;
        int firstAcknowledgeType = shareAcknowledgeBatch.acknowledgeTypes().get(0);
        for (int i = 1; i < shareAcknowledgeBatch.acknowledgeTypes().size(); i++) {
            if (shareAcknowledgeBatch.acknowledgeTypes().get(i) != firstAcknowledgeType) return false;
        }
        return true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Acknowledgements(");
        sb.append(acknowledgements);
        if (acknowledgeErrorCode != null) {
            sb.append(", errorCode=");
            sb.append(acknowledgeErrorCode.code());
        }
        sb.append(")");
        return sb.toString();
    }
}
