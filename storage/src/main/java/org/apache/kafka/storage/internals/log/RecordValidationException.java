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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.requests.ProduceResponse.RecordError;

import java.util.Collections;
import java.util.List;

public class RecordValidationException extends RuntimeException {
    private final ApiException invalidException;
    private final List<RecordError> recordErrors;

    public RecordValidationException(ApiException invalidException, List<RecordError> recordErrors) {
        super(invalidException);
        this.invalidException = invalidException;
        this.recordErrors = Collections.unmodifiableList(recordErrors);
    }

    public ApiException invalidException() {
        return invalidException;
    }

    public List<RecordError> recordErrors() {
        return recordErrors;
    }
}
