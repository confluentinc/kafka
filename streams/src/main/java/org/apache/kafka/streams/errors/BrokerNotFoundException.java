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
package org.apache.kafka.streams.errors;

/**
 * Indicates that none of the specified {@link org.apache.kafka.streams.StreamsConfig#BOOTSTRAP_SERVERS_CONFIG brokers}
 * could be found.
 *
 * @see org.apache.kafka.streams.StreamsConfig
 * @deprecated since 4.2 and should not be used any longer.
 */
@SuppressWarnings("unused")
@Deprecated
public class BrokerNotFoundException extends StreamsException {

    private static final long serialVersionUID = 1L;

    public BrokerNotFoundException(final String message) {
        super(message);
    }

    public BrokerNotFoundException(final String message, final Throwable throwable) {
        super(message, throwable);
    }

    public BrokerNotFoundException(final Throwable throwable) {
        super(throwable);
    }

}
