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

package org.apache.kafka.controller;

import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;


/**
 * The BrokerHeartbeatManager manages all the soft state associated with broker heartbeats.
 * Soft state is state which does not appear in the metadata log.  This state includes
 * things like the last time each broker sent us a heartbeat, and whether the broker is
 * trying to perform a controlled shutdown.
 *
 * Only the active controller has a BrokerHeartbeatManager, since only the active
 * controller handles broker heartbeats.  Standby controllers will create a heartbeat
 * manager as part of the process of activating.  This design minimizes the size of the
 * metadata partition by excluding heartbeats from it.  However, it does mean that after
 * a controller failover, we may take some extra time to fence brokers, since the new
 * active controller does not know when the last heartbeats were received from each.
 */
public class BrokerHeartbeatManager {
    class BrokerHeartbeatState {
        private final int id;
        private long lastContactNs;
        private BrokerHeartbeatState prev;
        private BrokerHeartbeatState next;
        private BrokerHeartbeatStateList list;

        BrokerHeartbeatState(int id) {
            this.id = id;
            this.lastContactNs = 0;
            this.prev = null;
            this.next = null;
            this.list = null;
        }

        int id() {
            return id;
        }

        boolean hasValidSession() {
            return sessionTimeoutNs() >= time.nanoseconds();
        }

        long sessionTimeoutNs() {
            return lastContactNs + sessionTimeoutNs;
        }

        void setLastContactNs(long lastContactNs) {
            if (list == null) {
                this.lastContactNs = lastContactNs;
            } else {
                list.remove(this);
                this.lastContactNs = lastContactNs;
                list.add(this);
            }
        }

        void setFenced(boolean fenced) {
            if (fenced) {
                if (list == unfenced) {
                    unfenced.remove(this);
                }
            } else if (list == null) {
                unfenced.add(this);
            } else {
                throw new RuntimeException("Can't add broker " + id + " to the " +
                    "fenced list since it is already in list " + list.name);
            }
        }
    }

    class BrokerHeartbeatStateList {
        /**
         * The name of the list.
         */
        private final String name;

        /**
         * The head of the list of unfenced brokers.  The list is sorted in ascending order
         * of last contact time.
         */
        private final BrokerHeartbeatState head;

        BrokerHeartbeatStateList(String name) {
            this.name = name;
            this.head = new BrokerHeartbeatState(-1);
            head.prev = head;
            head.next = head;
        }

        /**
         * Return the head of the list, or null if the list is empty.
         */
        BrokerHeartbeatState first() {
            BrokerHeartbeatState result = head.next;
            return result == head ? null : result;
        }

        /**
         * Add the broker to the list. We start looking for a place to put it at the end
         * of the list.
         */
        void add(BrokerHeartbeatState broker) {
            BrokerHeartbeatState cur = head.prev;
            while (true) {
                if (cur == head || cur.lastContactNs <= broker.lastContactNs) {
                    broker.next = cur.next;
                    cur.next.prev = broker;
                    broker.prev = cur;
                    cur.next = broker;
                    broker.list = this;
                    break;
                }
                cur = cur.prev;
            }
        }

        /**
         * Remove a broker from the list.
         */
        void remove(BrokerHeartbeatState broker) {
            if (broker.list != this) {
                throw new RuntimeException(broker + " is not in the " + name + " list.");
            }
            broker.list = null;
            broker.prev.next = broker.next;
            broker.next.prev = broker.prev;
            broker.prev = null;
            broker.next = null;
        }

        BrokerHeartbeatStateIterator iterator() {
            return new BrokerHeartbeatStateIterator(head);
        }
    }

    static class BrokerHeartbeatStateIterator implements Iterator<BrokerHeartbeatState> {
        private final BrokerHeartbeatState head;
        private BrokerHeartbeatState cur;

        BrokerHeartbeatStateIterator(BrokerHeartbeatState head) {
            this.head = head;
            this.cur = head;
        }

        @Override
        public boolean hasNext() {
            return cur.next != head;
        }

        @Override
        public BrokerHeartbeatState next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            BrokerHeartbeatState result = cur.next;
            cur = cur.next;
            return result;
        }
    }

    private final Logger log;

    /**
     * The Kafka clock object to use.
     */
    private final Time time;

    /**
     * The broker session timeout in nanoseconds.
     */
    private final long sessionTimeoutNs;

    /**
     * Maps broker IDs to heartbeat states.
     */
    private final HashMap<Integer, BrokerHeartbeatState> brokers;

    /**
     * The list of unfenced brokers, sorted by last contact time.
     */
    private final BrokerHeartbeatStateList unfenced;

    BrokerHeartbeatManager(LogContext logContext,
                           Time time,
                           long sessionTimeoutNs) {
        this.log = logContext.logger(ClusterControlManager.class);
        this.time = time;
        this.sessionTimeoutNs = sessionTimeoutNs;
        this.brokers = new HashMap<>();
        this.unfenced = new BrokerHeartbeatStateList("unfenced");
    }

    // VisibleForTesting
    Time time() {
        return time;
    }

    // VisibleForTesting
    BrokerHeartbeatStateList unfenced() {
        return unfenced;
    }

    /**
     * Remove broker state.
     *
     * @param brokerId      The broker ID to remove.
     */
    void remove(int brokerId) {
        BrokerHeartbeatState broker = brokers.remove(brokerId);
        if (broker != null && broker.list != null) {
            broker.list.remove(broker);
        }
    }

    /**
     * Check if the given broker has a valid session.
     *
     * @param brokerId      The broker ID to check.
     *
     * @return              True if the given broker heartbeated recently enough
     *                      that its session is still current.
     */
    boolean hasValidSession(int brokerId) {
        BrokerHeartbeatState broker = brokers.get(brokerId);
        if (broker == null) return false;
        return broker.hasValidSession();
    }

    /**
     * Update broker state, including lastContactNs.
     *
     * @param brokerId      The broker ID.
     * @param fenced        True only if the broker is currently fenced.
     */
    void touch(int brokerId, boolean fenced) {
        BrokerHeartbeatState broker = brokers.computeIfAbsent(brokerId,
                    __ -> new BrokerHeartbeatState(brokerId));
        broker.setLastContactNs(time.nanoseconds());
        broker.setFenced(fenced);
    }

    /**
     * Return the time in monotonic nanoseconds at which we should check if a broker 
     * session needs to be expired.
     */
    long nextCheckTimeNs() {
        BrokerHeartbeatState broker = unfenced.first();
        if (broker == null) {
            return Long.MAX_VALUE;
        } else {
            return broker.sessionTimeoutNs();
        }
    }

    /**
     * Fenced the broker which was least recently contacted, if needed.
     *
     * @return      -1 if no broker was fenced; the broker ID otherwise.  If the return
     *              value is non-negative, this function should be called again.
     */
    int maybeFenceLeastRecentlyContacted() {
        BrokerHeartbeatState broker = unfenced.first();
        if (broker == null || broker.hasValidSession()) {
            return -1;
        } else {
            broker.setFenced(true);
            return broker.id();
        }
    }

    /**
     * Place replicas on unfenced brokers.
     *
     * @param numPartitions     The number of partitions to place.
     * @param numReplicas       The number of replicas for each partition.
     * @param idToRack          A function mapping broker id to broker rack.
     * @param policy            The replica placement policy to use.
     *
     * @return                  A list of replica lists.
     *
     * @throws InvalidReplicationFactorException    If too many replicas were requested.
     */
    public List<List<Integer>> placeReplicas(int numPartitions, short numReplicas,
                                             Function<Integer, String> idToRack,
                                             ReplicaPlacementPolicy policy) {
        List<Integer> allActive = new ArrayList<>();
        Map<String, List<Integer>> activeByRack = new HashMap<>();
        for (Iterator<BrokerHeartbeatState> iter = unfenced.iterator(); iter.hasNext(); ) {
            int brokerId = iter.next().id();
            allActive.add(brokerId);
            String rack = idToRack.apply(brokerId);
            if (rack != null) {
                activeByRack.computeIfAbsent(rack, __ -> new ArrayList<>()).add(brokerId);
            }
        }
        return policy.createPlacement(
            numPartitions, numReplicas, allActive, activeByRack);
    }
}
