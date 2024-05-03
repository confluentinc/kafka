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

package org.apache.kafka.server.group.share;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public class NoOpShareStatePersister implements Persister {

  private static final Logger log = LoggerFactory.getLogger(NoOpShareStatePersister.class);


  @Override
  public CompletableFuture<InitializeShareGroupStateResult> initializeState(InitializeShareGroupStateParameters request) {
    banner("initializeState");
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<ReadShareGroupStateResult> readState(ReadShareGroupStateParameters request) {
    banner("readState");
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<WriteShareGroupStateResult> writeState(WriteShareGroupStateParameters request) {
    banner("writeState");
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<DeleteShareGroupStateResult> deleteState(DeleteShareGroupStateParameters request) {
    banner("deleteState");
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<ReadShareGroupOffsetsStateResult> readOffsets(ReadShareGroupOffsetsStateParameters request) {
    banner("readOffsets");
    return CompletableFuture.completedFuture(null);
  }

  private static void banner(String method) {
    log.info(method + " called on NoOpShareStatePersister. " +
        "Nothing would be read from or persisted to topic. " +
        "If this was unintended, please use some other implementations of Persister interface.");
  }
}
