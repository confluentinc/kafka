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

package org.apache.kafka.server.share;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.requests.ShareFetchMetadata;
import org.apache.kafka.common.utils.ImplicitLinkedHashCollection;

/**
 * Caches share sessions.
 * <p>
 * See tryEvict for an explanation of the cache eviction strategy.
 * <p>
 * The ShareSessionCache is thread-safe because all of its methods are synchronized.
 * Note that individual share sessions have their own locks which are separate from the
 * ShareSessionCache lock.  In order to avoid deadlock, the ShareSessionCache lock
 * must never be acquired while an individual ShareSession lock is already held.
 */
public class ShareSessionCache {
  private final int maxEntries;
  private final long evictionMs;
  private long numPartitions = 0;

  // A map of session key to ShareSession.
  private Map<ShareSessionKey, ShareSession> sessions = new HashMap<>();

  // Maps last used times to sessions.

  private TreeMap<LastUsedKey, ShareSession> lastUsed = new TreeMap<>();

  // Visible for testing
  public TreeMap<LastUsedKey, ShareSession> lastUsed() {
    return lastUsed;
  }

  public ShareSessionCache(int maxEntries, long evictionMs) {
    this.maxEntries = maxEntries;
    this.evictionMs = evictionMs;
  }

  /**
   * Get a session by session key.
   *
   * @param key The share session key.
   * @return The session, or None if no such session was found.
   */
  public ShareSession get(ShareSessionKey key) {
    synchronized (this) {
      return sessions.getOrDefault(key, null);
    }
  }

  /**
   * Get the number of entries currently in the share session cache.
   */
  public int size() {
    synchronized (this) {
      return sessions.size();
    }
  }

  public long totalPartitions() {
    synchronized (this) {
      return numPartitions;
    }
  }

  public ShareSession remove(ShareSessionKey key) {
    synchronized (this) {
      ShareSession session = get(key);
      if (session != null)
        return remove(session);
      return null;
    }
  }

  /**
   * Remove an entry from the session cache.
   *
   * @param session The session.
   * @return The removed session, or None if there was no such session.
   */
  public ShareSession remove(ShareSession session) {
    synchronized (this) {
      synchronized (session) {
        lastUsed.remove(session.lastUsedKey());
      }
      ShareSession removeResult = sessions.remove(session.key());
      if (removeResult != null) {
        numPartitions = numPartitions - session.cachedSize();
      }
      return removeResult;
    }
  }

  /**
   * Update a session's position in the lastUsed tree.
   *
   * @param session  The session.
   * @param now      The current time in milliseconds.
   */
  public void touch(ShareSession session, long now) {
    synchronized (this) {
      synchronized (session) {
        // Update the lastUsed map.
        lastUsed.remove(session.lastUsedKey());
        session.lastUsedMs(now);
        lastUsed.put(session.lastUsedKey(), session);

        int oldSize = session.cachedSize();
        if (oldSize != -1) {
          numPartitions = numPartitions - oldSize;
        }
        session.cachedSize(session.size());
        numPartitions = numPartitions + session.cachedSize();
      }
    }
  }

  /**
   * Try to evict an entry from the session cache.
   * <p>
   * A proposed new element A may evict an existing element B if:
   * B is considered "stale" because it has been inactive for a long time.
   *
   * @param now        The current time in milliseconds.
   * @return           True if an entry was evicted; false otherwise.
   */
  public boolean tryEvict(long now) {
    synchronized (this) {
      // Try to evict an entry which is stale.
      Map.Entry<LastUsedKey, ShareSession> lastUsedEntry = lastUsed.firstEntry();
      if (lastUsedEntry == null) {
        return false;
      } else if (now - lastUsedEntry.getKey().lastUsedMs() > evictionMs) {
        ShareSession session = lastUsedEntry.getValue();
        remove(session);
        return true;
      }
      return false;
    }
  }

  public ShareSessionKey maybeCreateSession(String groupId, Uuid memberId, long now, int size, ImplicitLinkedHashCollection<CachedSharePartition> partitionMap) {
    synchronized (this) {
      if (sessions.size() < maxEntries || tryEvict(now)) {
        ShareSession session = new ShareSession(new ShareSessionKey(groupId, memberId), partitionMap,
            now, now, ShareFetchMetadata.nextEpoch(ShareFetchMetadata.INITIAL_EPOCH));
        sessions.put(session.key(), session);
        touch(session, now);
        return session.key();
      }
      return null;
    }
  }
}
