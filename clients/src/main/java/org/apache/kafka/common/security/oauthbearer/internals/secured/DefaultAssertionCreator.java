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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.Time;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.jose4j.jws.JsonWebSignature;

import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * An {@link AssertionCreator} that creates the assertion from a private key in the configuration.
 */
public class DefaultAssertionCreator implements AssertionCreator {

    private final Time time;
    private final Supplier<String> privateKeySupplier;
    private final String privateKeyId;
    private final String privateKeySigningAlgorithm;

    public DefaultAssertionCreator(Time time,
                                   Supplier<String> privateKeySupplier,
                                   String privateKeyId,
                                   String privateKeySigningAlgorithm) {
        this.time = time;
        this.privateKeySupplier = privateKeySupplier;
        this.privateKeyId = privateKeyId;
        this.privateKeySigningAlgorithm = privateKeySigningAlgorithm;
    }

    @Override
    public String create(Map<String, Object> claims) {
        try {
            String privateKeyContents = privateKeySupplier.get()
                .replace("-----BEGIN PRIVATE KEY-----", "")
                .replace("-----END PRIVATE KEY-----", "")
                .replaceAll("\\s", "");

            byte[] pkcs8EncodedBytes = Base64.getDecoder().decode(privateKeyContents.getBytes(StandardCharsets.UTF_8));
            PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(pkcs8EncodedBytes);
            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            PrivateKey privateKey = keyFactory.generatePrivate(keySpec);

            ObjectMapper mapper = new ObjectMapper();
            String payload = mapper.writeValueAsString(augmentedClaims(claims));

            JsonWebSignature jws = new JsonWebSignature();
            jws.setKey(privateKey);
            jws.setKeyIdHeaderValue(privateKeyId);
            jws.setAlgorithmHeaderValue(privateKeySigningAlgorithm);
            jws.setPayload(payload);
            return jws.getCompactSerialization();
        } catch (Exception e) {
            throw new KafkaException("An error was thrown when creating the OAuth assertion", e);
        }
    }

    Map<String, Object> augmentedClaims(Map<String, Object> claims) {
        long currentTimeSecs = time.milliseconds() / 1000;
        long expirationSecs = currentTimeSecs + Duration.ofMinutes(60).toSeconds();

        Map<String, Object> augmentedClaims = new HashMap<>(claims);

        if (!augmentedClaims.containsKey("iat"))
            augmentedClaims.put("iat", currentTimeSecs);

        if (!augmentedClaims.containsKey("exp"))
            augmentedClaims.put("exp", expirationSecs);

        return augmentedClaims;
    }
}
