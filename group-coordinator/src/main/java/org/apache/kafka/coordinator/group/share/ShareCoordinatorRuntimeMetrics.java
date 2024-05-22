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

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.coordinator.group.metrics.CoordinatorRuntimeMetrics;
import org.apache.kafka.coordinator.group.runtime.CoordinatorRuntime;

import java.util.Objects;
import java.util.function.Supplier;

//todo smjn- need to add metrics. Currently placeholder for adhering to coordinator API
public class ShareCoordinatorRuntimeMetrics implements CoordinatorRuntimeMetrics {

  private final Metrics metrics;

  public ShareCoordinatorRuntimeMetrics(Metrics metrics) {
    this.metrics = Objects.requireNonNull(metrics);
  }

  @Override
  public void recordPartitionStateChange(CoordinatorRuntime.CoordinatorState oldState, CoordinatorRuntime.CoordinatorState newState) {

  }

  @Override
  public void recordPartitionLoadSensor(long startTimeMs, long endTimeMs) {

  }

  @Override
  public void recordEventQueueTime(long durationMs) {

  }

  @Override
  public void recordEventQueueProcessingTime(long durationMs) {

  }

  @Override
  public void recordThreadIdleRatio(double ratio) {

  }

  @Override
  public void registerEventQueueSizeGauge(Supplier<Integer> sizeSupplier) {

  }

  @Override
  public void close() throws Exception {

  }
}
