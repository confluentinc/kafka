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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Options for {@link Admin#listConsumerGroups()}.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class ListConsumerGroupsOptions extends AbstractOptions<ListConsumerGroupsOptions> {

    private Set<ConsumerGroupState> states = Collections.emptySet();

    private Set<GroupType> types = Collections.emptySet();

    /**
     * If states is set, only groups in these states will be returned by listConsumerGroups().
     * Otherwise, all groups are returned.
     * This operation is supported by brokers with version 2.6.0 or later.
     */
    public ListConsumerGroupsOptions inStates(Set<ConsumerGroupState> states) {
        this.states = (states == null || states.isEmpty()) ? Collections.emptySet() : new HashSet<>(states);
        return this;
    }

    /**
     * If types is set, only groups of these types will be returned by listConsumerGroups().
     * Otherwise, all groups are returned.
     */
    public ListConsumerGroupsOptions withTypes(Set<GroupType> types) {
        this.types = (types == null || types.isEmpty()) ? Collections.emptySet() : new HashSet<>(types);
        return this;
    }

    /**
     * Returns the list of States that are requested or empty if no states have been specified.
     */
    public Set<ConsumerGroupState> states() {
        return states;
    }

    /**
     * Returns the list of group types that are requested or empty if no types have been specified.
     */
    public Set<GroupType> types() {
        return types;
    }
}
