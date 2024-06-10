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

package kafka.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import kafka.server.SharePartitionManager.ErroneousAndValidPartitionData;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.message.ShareFetchResponseData.PartitionData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ShareFetchMetadata;
import org.apache.kafka.common.requests.ShareFetchRequest;
import org.apache.kafka.common.requests.ShareFetchRequest.SharePartitionData;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.share.CachedSharePartition;
import org.apache.kafka.server.share.ShareSession;
import org.apache.kafka.server.share.ShareSessionCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * The context for a share session fetch request.
 */
public class ShareSessionContext extends ShareFetchContext {

  private final Time time;
  private ShareSessionCache cache;
  private final ShareFetchMetadata reqMetadata;
  private Map<TopicIdPartition, SharePartitionData> shareFetchData;
  private final boolean isSubsequent;
  private ShareSession session;

  private final Logger log = LoggerFactory.getLogger(ShareSessionContext.class);

  /**
   * The share fetch context for the first request that starts a share session.
   *
   * @param time               The clock to use.
   * @param cache              The share session cache.
   * @param reqMetadata        The request metadata.
   * @param shareFetchData     The share partition data from the share fetch request.
   */
  public ShareSessionContext(Time time, ShareSessionCache cache, ShareFetchMetadata reqMetadata,
      Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> shareFetchData) {
    this.time = time;
    this.cache = cache;
    this.reqMetadata = reqMetadata;
    this.shareFetchData = shareFetchData;
    this.isSubsequent = false;
  }

  /**
   * The share fetch context for a subsequent request that utilizes an existing share session.
   *
   * @param time         The clock to use.
   * @param reqMetadata  The request metadata.
   * @param session      The subsequent fetch request session.
   */
  public ShareSessionContext(Time time, ShareFetchMetadata reqMetadata, ShareSession session) {
    this.time = time;
    this.reqMetadata = reqMetadata;
    this.session = session;
    this.isSubsequent = true;
  }

  public Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> shareFetchData() {
    return shareFetchData;
  }

  public boolean isSubsequent() {
    return isSubsequent;
  }

  public ShareSession session() {
    return session;
  }

  @Override
  ShareFetchResponse throttleResponse(int throttleTimeMs) {
    if (!isSubsequent) {
      return new ShareFetchResponse(ShareFetchResponse.toMessage(Errors.NONE, throttleTimeMs,
          Collections.emptyIterator(), Collections.emptyList()));
    } else {
      int expectedEpoch = ShareFetchMetadata.nextEpoch(reqMetadata.epoch());
      int sessionEpoch;
      synchronized (session) {
        sessionEpoch = session.epoch;
      }
      if (sessionEpoch != expectedEpoch) {
        log.debug("Subsequent share session {} expected epoch {}, but got {}. " +
            "Possible duplicate request.", session.key(), expectedEpoch, sessionEpoch);
        return new ShareFetchResponse(ShareFetchResponse.toMessage(Errors.INVALID_SHARE_SESSION_EPOCH,
            throttleTimeMs, Collections.emptyIterator(), Collections.emptyList()));
      } else
        return new ShareFetchResponse(ShareFetchResponse.toMessage(Errors.NONE, throttleTimeMs,
            Collections.emptyIterator(), Collections.emptyList()));
    }
  }

  // Iterator that goes over the given partition map and selects partitions that need to be included in the response.
  // If updateShareContextAndRemoveUnselected is set to true, the share context will be updated for the selected
  // partitions and also remove unselected ones as they are encountered.
  private class PartitionIterator implements
      Iterator<Entry<TopicIdPartition, PartitionData>> {
    private final Iterator<Map.Entry<TopicIdPartition, ShareFetchResponseData.PartitionData>> iterator;
    private final boolean updateShareContextAndRemoveUnselected;
    private Map.Entry<TopicIdPartition, ShareFetchResponseData.PartitionData> nextElement;


    public PartitionIterator(Iterator<Map.Entry<TopicIdPartition, ShareFetchResponseData.PartitionData>> iterator, boolean updateShareContextAndRemoveUnselected) {
      this.iterator = iterator;
      this.updateShareContextAndRemoveUnselected = updateShareContextAndRemoveUnselected;
    }

    @Override
    public boolean hasNext() {
      while ((nextElement == null) && iterator.hasNext()) {
        Map.Entry<TopicIdPartition, ShareFetchResponseData.PartitionData> element = iterator.next();
        TopicIdPartition topicPart = element.getKey();
        ShareFetchResponseData.PartitionData respData = element.getValue();
        CachedSharePartition cachedPart = session.partitionMap().find(new CachedSharePartition(topicPart));
        boolean mustRespond = cachedPart.maybeUpdateResponseData(respData, updateShareContextAndRemoveUnselected);
        if (mustRespond) {
          nextElement = element;
          if (updateShareContextAndRemoveUnselected && ShareFetchResponse.recordsSize(respData) > 0) {
            // Session.partitionMap is of type ImplicitLinkedHashCollection<> which tracks the order of insertion of elements.
            // Since, we are updating an element in this case, we need to perform a remove and then a mustAdd to maintain the correct order
            session.partitionMap().remove(cachedPart);
            session.partitionMap().mustAdd(cachedPart);
          }
        } else {
          if (updateShareContextAndRemoveUnselected) {
            iterator.remove();
          }
        }
      }
      return nextElement != null;
    }

    @Override
    public Map.Entry<TopicIdPartition, ShareFetchResponseData.PartitionData> next() {
      if (!hasNext()) throw new NoSuchElementException();
      Map.Entry<TopicIdPartition, ShareFetchResponseData.PartitionData> element = nextElement;
      nextElement = null;
      return element;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  int responseSize(LinkedHashMap<TopicIdPartition, PartitionData> updates,
      short version) {
    if (!isSubsequent)
      return ShareFetchResponse.sizeOf(version, updates.entrySet().iterator());
    else {
      synchronized (session) {
        int expectedEpoch = ShareFetchMetadata.nextEpoch(reqMetadata.epoch());
        if (session.epoch != expectedEpoch) {
          return ShareFetchResponse.sizeOf(version, Collections.emptyIterator());
        } else {
          // Pass the partition iterator which updates neither the share fetch context nor the partition map.
          return ShareFetchResponse.sizeOf(version, new PartitionIterator(updates.entrySet().iterator(), false));
        }
      }
    }
  }

  @Override
  ShareFetchResponse updateAndGenerateResponseData(String groupId, Uuid memberId,
      LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates) {
    if (!isSubsequent) {
      return new ShareFetchResponse(ShareFetchResponse.toMessage(
          Errors.NONE, 0, updates.entrySet().iterator(), Collections.emptyList()));
    } else {
      int expectedEpoch = ShareFetchMetadata.nextEpoch(reqMetadata.epoch());
      int sessionEpoch;
      synchronized (session) {
        sessionEpoch = session.epoch;
      }
      if (session.epoch != expectedEpoch) {
        log.info("Subsequent share session {} expected epoch {}, but got {}. Possible duplicate request.",
            session.key(), expectedEpoch, sessionEpoch);
        return new ShareFetchResponse(ShareFetchResponse.toMessage(Errors.INVALID_SHARE_SESSION_EPOCH,
            0, Collections.emptyIterator(), Collections.emptyList()));
      } else {
        // Iterate over the update list using PartitionIterator. This will prune updates which don't need to be sent
        Iterator<Map.Entry<TopicIdPartition, ShareFetchResponseData.PartitionData>> partitionIterator = new PartitionIterator(
            updates.entrySet().iterator(), true);
        while (partitionIterator.hasNext()) {
          partitionIterator.next();
        }
        log.debug("Subsequent share session context with session key {} returning {}", session.key(),
            partitionsToLogString(updates.keySet()));
        return new ShareFetchResponse(ShareFetchResponse.toMessage(
            Errors.NONE, 0, updates.entrySet().iterator(), Collections.emptyList()));
      }
    }
  }

  @Override
  ErroneousAndValidPartitionData getErroneousAndValidTopicIdPartitions() {
    if (!isSubsequent) {
      return new ErroneousAndValidPartitionData(shareFetchData);
    } else {
      List<Tuple2<TopicIdPartition, PartitionData>> erroneous = new ArrayList<>();
      List<Tuple2<TopicIdPartition, ShareFetchRequest.SharePartitionData>> valid = new ArrayList<>();
      // Take the session lock and iterate over all the cached partitions.
      synchronized (session) {
        session.partitionMap().forEach(cachedSharePartition -> {
          TopicIdPartition topicIdPartition = new TopicIdPartition(cachedSharePartition.topicId(), new
              TopicPartition(cachedSharePartition.topic(), cachedSharePartition.partition()));
          ShareFetchRequest.SharePartitionData reqData = cachedSharePartition.reqData();
          if (topicIdPartition.topic() == null) {
            erroneous.add(new Tuple2<>(topicIdPartition, ShareFetchResponse.partitionResponse(topicIdPartition, Errors.UNKNOWN_TOPIC_ID)));
          } else {
            valid.add(new Tuple2<>(topicIdPartition, reqData));
          }
        });
        return new ErroneousAndValidPartitionData(erroneous, valid);
      }
    }
  }
}