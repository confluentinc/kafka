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
import java.util.Iterator;
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
        Iterator<Map.Entry<Long, AcknowledgeType>> iterator = acknowledgements.entrySet().iterator();
        int maxRecordsWithSameAcknowledgeType = 0;
        while (iterator.hasNext()) {
            Map.Entry<Long, AcknowledgeType> entry = iterator.next();
            if (currentBatch == null) {
                currentBatch = new ShareFetchRequestData.AcknowledgementBatch();
                currentBatch.setFirstOffset(entry.getKey());
                currentBatch.setLastOffset(entry.getKey());
                if (entry.getValue() != null) {
                    currentBatch.acknowledgeTypes().add(entry.getValue().id);
                } else {
                    currentBatch.acknowledgeTypes().add(ACKNOWLEDGE_TYPE_GAP);
                }
                maxRecordsWithSameAcknowledgeType++;
            } else {
                if (entry.getValue() != null) {
                    if (entry.getKey() == currentBatch.lastOffset() + 1) {
                        OptimiseAcknowledgeTypeArray optimiseAcknowledgeTypeArray = new OptimiseAcknowledgeTypeArray(currentBatch, batches, maxRecordsWithSameAcknowledgeType);

                        optimiseAcknowledgeTypeArray.maybeOptimiseAcknowledgementTypesForFetch(entry);

                        currentBatch = optimiseAcknowledgeTypeArray.currentShareFetchBatch;
                        batches = optimiseAcknowledgeTypeArray.shareFetchBatches;
                        maxRecordsWithSameAcknowledgeType = optimiseAcknowledgeTypeArray.maxRecordsWithSameAcknowledgeType;
                    } else {
                        if (OptimiseAcknowledgeTypeArray.maybeOptimiseForSingleAcknowledgeType(currentBatch)) {
                            // If the batch had a single acknowledgement type, we optimise the array independent
                            // of the number of records.
                            currentBatch.acknowledgeTypes().subList(1, currentBatch.acknowledgeTypes().size()).clear();
                        }
                        batches.add(currentBatch);

                        currentBatch = new ShareFetchRequestData.AcknowledgementBatch();
                        currentBatch.setFirstOffset(entry.getKey());
                        currentBatch.setLastOffset(entry.getKey());
                        currentBatch.acknowledgeTypes().add(entry.getValue().id);
                        maxRecordsWithSameAcknowledgeType = 1;
                    }
                } else {
                    if (entry.getKey() == currentBatch.lastOffset() + 1) {
                        OptimiseAcknowledgeTypeArray optimiseAcknowledgeTypeArray = new OptimiseAcknowledgeTypeArray(currentBatch, batches, maxRecordsWithSameAcknowledgeType);

                        optimiseAcknowledgeTypeArray.maybeOptimiseAcknowledgementTypesForFetch(entry);

                        currentBatch = optimiseAcknowledgeTypeArray.currentShareFetchBatch;
                        batches = optimiseAcknowledgeTypeArray.shareFetchBatches;
                        maxRecordsWithSameAcknowledgeType = optimiseAcknowledgeTypeArray.maxRecordsWithSameAcknowledgeType;
                    } else {
                        if (OptimiseAcknowledgeTypeArray.maybeOptimiseForSingleAcknowledgeType(currentBatch)) {
                            currentBatch.acknowledgeTypes().subList(1, currentBatch.acknowledgeTypes().size()).clear();
                        }
                        batches.add(currentBatch);

                        currentBatch = new ShareFetchRequestData.AcknowledgementBatch();
                        currentBatch.setFirstOffset(entry.getKey());
                        currentBatch.setLastOffset(entry.getKey());
                        currentBatch.acknowledgeTypes().add(ACKNOWLEDGE_TYPE_GAP);
                        maxRecordsWithSameAcknowledgeType = 1;
                    }
                }
            }
        }
        if (OptimiseAcknowledgeTypeArray.maybeOptimiseForSingleAcknowledgeType(currentBatch)) {
            currentBatch.acknowledgeTypes().subList(1, currentBatch.acknowledgeTypes().size()).clear();
        }
        batches.add(currentBatch);
        return batches;
    }

    public List<ShareAcknowledgeRequestData.AcknowledgementBatch> getShareAcknowledgeBatches() {
        List<ShareAcknowledgeRequestData.AcknowledgementBatch> batches = new ArrayList<>();
        if (acknowledgements.isEmpty()) return batches;
        ShareAcknowledgeRequestData.AcknowledgementBatch currentBatch = null;
        Iterator<Map.Entry<Long, AcknowledgeType>> iterator = acknowledgements.entrySet().iterator();
        int maxRecordsWithSameAcknowledgeType = 0;
        while (iterator.hasNext()) {
            Map.Entry<Long, AcknowledgeType> entry = iterator.next();
            if (currentBatch == null) {
                currentBatch = new ShareAcknowledgeRequestData.AcknowledgementBatch();
                currentBatch.setFirstOffset(entry.getKey());
                currentBatch.setLastOffset(entry.getKey());
                if (entry.getValue() != null) {
                    currentBatch.acknowledgeTypes().add(entry.getValue().id);
                } else {
                    currentBatch.acknowledgeTypes().add(ACKNOWLEDGE_TYPE_GAP);
                }
                maxRecordsWithSameAcknowledgeType++;
            } else {
                if (entry.getValue() != null) {
                    if (entry.getKey() == currentBatch.lastOffset() + 1) {
                        OptimiseAcknowledgeTypeArray optimiseAcknowledgeTypeArray = new OptimiseAcknowledgeTypeArray(currentBatch, batches, maxRecordsWithSameAcknowledgeType);

                        optimiseAcknowledgeTypeArray.maybeOptimiseAcknowledgementTypesForAcknowledge(entry);

                        currentBatch = optimiseAcknowledgeTypeArray.currentShareAcknowledgeBatch;
                        batches = optimiseAcknowledgeTypeArray.shareAcknowledgeBatches;
                        maxRecordsWithSameAcknowledgeType = optimiseAcknowledgeTypeArray.maxRecordsWithSameAcknowledgeType;
                    } else {
                        if (OptimiseAcknowledgeTypeArray.maybeOptimiseForSingleAcknowledgeType(currentBatch)) {
                            currentBatch.acknowledgeTypes().subList(1, currentBatch.acknowledgeTypes().size()).clear();
                        }
                        batches.add(currentBatch);

                        currentBatch = new ShareAcknowledgeRequestData.AcknowledgementBatch();
                        currentBatch.setFirstOffset(entry.getKey());
                        currentBatch.setLastOffset(entry.getKey());
                        currentBatch.acknowledgeTypes().add(entry.getValue().id);
                    }
                } else {
                    if (entry.getKey() == currentBatch.lastOffset() + 1) {
                        OptimiseAcknowledgeTypeArray optimiseAcknowledgeTypeArray = new OptimiseAcknowledgeTypeArray(currentBatch, batches, maxRecordsWithSameAcknowledgeType);

                        optimiseAcknowledgeTypeArray.maybeOptimiseAcknowledgementTypesForAcknowledge(entry);

                        currentBatch = optimiseAcknowledgeTypeArray.currentShareAcknowledgeBatch;
                        batches = optimiseAcknowledgeTypeArray.shareAcknowledgeBatches;
                        maxRecordsWithSameAcknowledgeType = optimiseAcknowledgeTypeArray.maxRecordsWithSameAcknowledgeType;
                    } else {
                        if (OptimiseAcknowledgeTypeArray.maybeOptimiseForSingleAcknowledgeType(currentBatch)) {
                            currentBatch.acknowledgeTypes().subList(1, currentBatch.acknowledgeTypes().size()).clear();
                        }
                        batches.add(currentBatch);

                        currentBatch = new ShareAcknowledgeRequestData.AcknowledgementBatch();
                        currentBatch.setFirstOffset(entry.getKey());
                        currentBatch.setLastOffset(entry.getKey());
                        currentBatch.acknowledgeTypes().add(ACKNOWLEDGE_TYPE_GAP);
                    }
                }
            }
        }
        if (OptimiseAcknowledgeTypeArray.maybeOptimiseForSingleAcknowledgeType(currentBatch)) {
            currentBatch.acknowledgeTypes().subList(1, currentBatch.acknowledgeTypes().size()).clear();
        }
        batches.add(currentBatch);
        return batches;
    }

    private static class OptimiseAcknowledgeTypeArray {
        public ShareFetchRequestData.AcknowledgementBatch currentShareFetchBatch;
        public List<ShareFetchRequestData.AcknowledgementBatch> shareFetchBatches;
        public ShareAcknowledgeRequestData.AcknowledgementBatch currentShareAcknowledgeBatch;
        public List<ShareAcknowledgeRequestData.AcknowledgementBatch> shareAcknowledgeBatches;
        public int maxRecordsWithSameAcknowledgeType;


        public OptimiseAcknowledgeTypeArray(ShareFetchRequestData.AcknowledgementBatch currentBatch,
                                            List<ShareFetchRequestData.AcknowledgementBatch> batches,
                                            int maxRecordsWithSameAcknowledgeType) {
            this.currentShareFetchBatch = currentBatch.duplicate();
            this.shareFetchBatches = new ArrayList<>(batches);
            this.maxRecordsWithSameAcknowledgeType = maxRecordsWithSameAcknowledgeType;
            this.currentShareAcknowledgeBatch = null;
            this.shareAcknowledgeBatches = null;
        }

        public OptimiseAcknowledgeTypeArray(ShareAcknowledgeRequestData.AcknowledgementBatch currentBatch,
                                            List<ShareAcknowledgeRequestData.AcknowledgementBatch> batches,
                                            int maxRecordsWithSameAcknowledgeType) {
            this.currentShareFetchBatch = null;
            this.shareFetchBatches = null;
            this.maxRecordsWithSameAcknowledgeType = maxRecordsWithSameAcknowledgeType;
            this.currentShareAcknowledgeBatch = currentBatch;
            this.shareAcknowledgeBatches = batches;
        }

        /**
         * If the batch had a single acknowledgement type, we optimise the array independent
         * of the number of records.
         */
        private static boolean maybeOptimiseForSingleAcknowledgeType(ShareFetchRequestData.AcknowledgementBatch shareFetchBatch) {
            if (shareFetchBatch == null || shareFetchBatch.acknowledgeTypes().size() == 1) return false;
            int firstAcknowledgeType = shareFetchBatch.acknowledgeTypes().get(0);
            for (int i = 1; i < shareFetchBatch.acknowledgeTypes().size(); i++) {
                if (shareFetchBatch.acknowledgeTypes().get(i) != firstAcknowledgeType) return false;
            }
            return true;
        }

        private static boolean maybeOptimiseForSingleAcknowledgeType(ShareAcknowledgeRequestData.AcknowledgementBatch shareAcknowledgeBatch) {
            if (shareAcknowledgeBatch == null || shareAcknowledgeBatch.acknowledgeTypes().size() == 1) return false;
            int firstAcknowledgeType = shareAcknowledgeBatch.acknowledgeTypes().get(0);
            for (int i = 1; i < shareAcknowledgeBatch.acknowledgeTypes().size(); i++) {
                if (shareAcknowledgeBatch.acknowledgeTypes().get(i) != firstAcknowledgeType) return false;
            }
            return true;
        }

        private void maybeOptimiseAcknowledgementTypesForFetch(Map.Entry<Long, AcknowledgeType> entry) {
            byte acknowledgeType = entry.getValue() == null ? ACKNOWLEDGE_TYPE_GAP : entry.getValue().id;
            int lastIndex = currentShareFetchBatch.acknowledgeTypes().size() - 1;
            // If we have a continuous set of records with the same acknowledgement type exceeding the default count,
            // then we optimise the batches to include only start and end offset and have only 1 acknowledge type in the array.
            if (acknowledgeType == currentShareFetchBatch.acknowledgeTypes().get(lastIndex) && maxRecordsWithSameAcknowledgeType >= MAX_RECORDS_WITH_SAME_ACKNOWLEDGE_TYPE) {
                if (lastIndex - maxRecordsWithSameAcknowledgeType + 1 >= 0) {
                    currentShareFetchBatch.acknowledgeTypes().subList(lastIndex - maxRecordsWithSameAcknowledgeType + 1, lastIndex + 1).clear();
                    long newStartOffset;
                    // Case when the batch had more records before the set of continuous records.
                    if (!currentShareFetchBatch.acknowledgeTypes().isEmpty()) {
                        currentShareFetchBatch.setLastOffset(currentShareFetchBatch.lastOffset() - maxRecordsWithSameAcknowledgeType);
                        // This is an additional optimisation to see if we are adding a batch containing a single acknowledgement type
                        // independent of the number of records.
                        if (maybeOptimiseForSingleAcknowledgeType(currentShareFetchBatch)) {
                            // Make the acknowledgeType array to have only 1 element
                            currentShareFetchBatch.acknowledgeTypes().subList(1, currentShareFetchBatch.acknowledgeTypes().size()).clear();
                        }
                        shareFetchBatches.add(currentShareFetchBatch);
                        newStartOffset = entry.getKey() - maxRecordsWithSameAcknowledgeType;
                    } else {
                        newStartOffset = currentShareFetchBatch.firstOffset();
                    }
                    currentShareFetchBatch = new ShareFetchRequestData.AcknowledgementBatch();
                    currentShareFetchBatch.setFirstOffset(newStartOffset);
                    currentShareFetchBatch.setLastOffset(entry.getKey());
                    maxRecordsWithSameAcknowledgeType++;
                    currentShareFetchBatch.acknowledgeTypes().add(acknowledgeType);
                } else {
                    // Case when the array was already optimised and we just need to update last offset.
                    currentShareFetchBatch.setLastOffset(entry.getKey());
                    maxRecordsWithSameAcknowledgeType++;
                }
            } else if (acknowledgeType == currentShareFetchBatch.acknowledgeTypes().get(lastIndex)) {
                // The maximum limit has not yet been reached, we increment the count and move ahead.
                maxRecordsWithSameAcknowledgeType++;
                currentShareFetchBatch.setLastOffset(entry.getKey());
                currentShareFetchBatch.acknowledgeTypes().add(acknowledgeType);
            } else {
                if (currentShareFetchBatch.acknowledgeTypes().size() == 1 && currentShareFetchBatch.lastOffset() > currentShareFetchBatch.firstOffset()) {
                    // Case when the array was already optimised to have single acknowledgement type and a different acknowledgement type is added.
                    // Now we have to split into 2 batches, the first one will be the optimised batch.
                    shareFetchBatches.add(currentShareFetchBatch);
                    currentShareFetchBatch = new ShareFetchRequestData.AcknowledgementBatch();
                    currentShareFetchBatch.setFirstOffset(entry.getKey());
                }
                maxRecordsWithSameAcknowledgeType = 1;
                currentShareFetchBatch.setLastOffset(entry.getKey());
                currentShareFetchBatch.acknowledgeTypes().add(acknowledgeType);
            }
        }

        private void maybeOptimiseAcknowledgementTypesForAcknowledge(Map.Entry<Long, AcknowledgeType> entry) {
            byte acknowledgeType = entry.getValue() == null ? ACKNOWLEDGE_TYPE_GAP : entry.getValue().id;
            int lastIndex = currentShareAcknowledgeBatch.acknowledgeTypes().size() - 1;
            // If we have a continuous set of records with the same acknowledgement type exceeding the default count,
            // then we optimise the batches to include only start and end offset and have only 1 acknowledge type in the array.
            if (acknowledgeType == currentShareAcknowledgeBatch.acknowledgeTypes().get(lastIndex) && maxRecordsWithSameAcknowledgeType >= MAX_RECORDS_WITH_SAME_ACKNOWLEDGE_TYPE) {
                if (lastIndex - maxRecordsWithSameAcknowledgeType + 1 >= 0) {
                    currentShareAcknowledgeBatch.acknowledgeTypes().subList(lastIndex - maxRecordsWithSameAcknowledgeType + 1, lastIndex + 1).clear();
                    long newStartOffset;
                    // Case when the batch had more records before the set of continuous records.
                    if (!currentShareAcknowledgeBatch.acknowledgeTypes().isEmpty()) {
                        currentShareAcknowledgeBatch.setLastOffset(currentShareAcknowledgeBatch.lastOffset() - maxRecordsWithSameAcknowledgeType);
                        // This is an additional optimisation to see if we are adding a batch containing a single acknowledgement type
                        // independent of the number of records.
                        if (maybeOptimiseForSingleAcknowledgeType(currentShareAcknowledgeBatch)) {
                            // Make the acknowledgeType array to have only 1 element
                            currentShareAcknowledgeBatch.acknowledgeTypes().subList(1, currentShareAcknowledgeBatch.acknowledgeTypes().size()).clear();
                        }
                        shareAcknowledgeBatches.add(currentShareAcknowledgeBatch);
                        newStartOffset = entry.getKey() - maxRecordsWithSameAcknowledgeType;
                    } else {
                        newStartOffset = currentShareAcknowledgeBatch.firstOffset();
                    }
                    currentShareAcknowledgeBatch = new ShareAcknowledgeRequestData.AcknowledgementBatch();
                    currentShareAcknowledgeBatch.setFirstOffset(newStartOffset);
                    currentShareAcknowledgeBatch.setLastOffset(entry.getKey());
                    maxRecordsWithSameAcknowledgeType++;
                    currentShareAcknowledgeBatch.acknowledgeTypes().add(acknowledgeType);
                } else {
                    // Case when the array was already optimised and we just need to update last offset.
                    currentShareAcknowledgeBatch.setLastOffset(entry.getKey());
                    maxRecordsWithSameAcknowledgeType++;
                }
            } else if (acknowledgeType == currentShareAcknowledgeBatch.acknowledgeTypes().get(lastIndex)) {
                // The maximum limit has not yet been reached, we increment the count and move ahead.
                maxRecordsWithSameAcknowledgeType++;
                currentShareAcknowledgeBatch.setLastOffset(entry.getKey());
                currentShareAcknowledgeBatch.acknowledgeTypes().add(acknowledgeType);
            } else {
                if (currentShareAcknowledgeBatch.acknowledgeTypes().size() == 1 && currentShareAcknowledgeBatch.lastOffset() > currentShareAcknowledgeBatch.firstOffset()) {
                    // Case when the array was already optimised to have single acknowledgement type and a different acknowledgement type is added.
                    // Now we have to split into 2 batches, the first one will be the optimised batch.
                    shareAcknowledgeBatches.add(currentShareAcknowledgeBatch);
                    currentShareAcknowledgeBatch = new ShareAcknowledgeRequestData.AcknowledgementBatch();
                    currentShareAcknowledgeBatch.setFirstOffset(entry.getKey());
                }
                maxRecordsWithSameAcknowledgeType = 1;
                currentShareAcknowledgeBatch.setLastOffset(entry.getKey());
                currentShareAcknowledgeBatch.acknowledgeTypes().add(acknowledgeType);
            }
        }

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
