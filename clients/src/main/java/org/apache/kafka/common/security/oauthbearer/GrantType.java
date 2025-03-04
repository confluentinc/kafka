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

package org.apache.kafka.common.security.oauthbearer;

import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum GrantType {

    // https://datatracker.ietf.org/doc/html/rfc6749#section-4.4.2
    CLIENT_CREDENTIALS("client_credentials"),
    // https://datatracker.ietf.org/doc/html/rfc7523#section-8.1
    JWT_BEARER("urn:ietf:params:oauth:grant-type:jwt-bearer");

    private final String value;

    GrantType(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    /**
     * Case-insensitive grant type lookup by the value.
     */
    public static GrantType fromValue(final String value) {
        for (GrantType grantType : values()) {
            if (grantType.value.equalsIgnoreCase(value))
                return grantType;
        }

        throw new IllegalArgumentException("Could not match the " + GrantType.class.getSimpleName() + " to the value \"" + value + "\"");
    }

    public static String[] validValueStrings() {
        Set<String> set = new TreeSet<>(Stream.of(values())
            .map(GrantType::value)
            .collect(Collectors.toUnmodifiableSet()));
        return set.toArray(String[]::new);
    }
}
