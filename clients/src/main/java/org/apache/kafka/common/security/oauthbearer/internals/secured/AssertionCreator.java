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

package org.apache.kafka.common.security.oauthbearer.internals.secured;

import java.security.NoSuchAlgorithmException;
import java.security.Signature;
import java.util.Map;

/**
 * {@code AssertionCreator} is used to create an OAuth assertion that can be used with different
 * grant types.
 */
public interface AssertionCreator {

    enum SigningAlgorithm {

        RS256("RS256", "SHA256withRSA"),
        ES256("ES256", "SHA256withECDSA");

        private final String name;
        private final String algorithm;

        SigningAlgorithm(String name, String algorithm) {
            this.name = name;
            this.algorithm = algorithm;
        }

        Signature newSignature() throws NoSuchAlgorithmException {
            return Signature.getInstance(algorithm);
        }

        static SigningAlgorithm fromName(final String name) {
            for (SigningAlgorithm signingAlgorithm : values()) {
                if (signingAlgorithm.name.equalsIgnoreCase(name))
                    return signingAlgorithm;
            }

            throw new IllegalArgumentException(String.format("Unsupported signing algorithm: %s", name));
        }
    }

    /**
     * Creates an OAuth assertion by converting the given claims into JWT and then signing them using
     * the appropriate algorithm.
     */
    String create(Map<String, Object> claims);
}
