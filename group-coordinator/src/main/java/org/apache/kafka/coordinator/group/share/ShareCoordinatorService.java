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

package org.apache.kafka.coordinator.group.share;

import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.record.BrokerCompressionType;

import java.util.Properties;

public class ShareCoordinatorService implements ShareCoordinator {
  private final int numPartitions;
  private final ShareCoordinatorConfig config;

  public ShareCoordinatorService(ShareCoordinatorConfig config) {
    this.numPartitions = 50;  //todo smjn: this should be configurable
    this.config = config;
  }

  @Override
  public int partitionFor(String key) {
    return Utils.abs(key.hashCode()) % numPartitions;
  }

  @Override
  public Properties shareGroupStateTopicConfigs() {
    Properties properties = new Properties();
    //todo smjn: taken directly from group coord - need to check validity of these configs
    properties.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE); // as defined in KIP-932
    properties.put(TopicConfig.COMPRESSION_TYPE_CONFIG, BrokerCompressionType.PRODUCER.name);
    properties.put(TopicConfig.SEGMENT_BYTES_CONFIG, config.shareCoordinatorStateTopicSegmentBytes);
    return properties;
  }
}
