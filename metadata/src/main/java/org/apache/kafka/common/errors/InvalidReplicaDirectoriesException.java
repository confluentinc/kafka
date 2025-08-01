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
package org.apache.kafka.common.errors;

import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;

/**
 * A record was encountered where the number of directories does not match the number of replicas.
 */
public class InvalidReplicaDirectoriesException extends InvalidMetadataException {
    private static final String ERR_MSG = "The lengths for replicas and directories do not match: ";

    private static final long serialVersionUID = 1L;

    public InvalidReplicaDirectoriesException(PartitionRecord record) {
        super(ERR_MSG + record);
    }

    public InvalidReplicaDirectoriesException(PartitionChangeRecord record) {
        super(ERR_MSG + record);
    }
}
