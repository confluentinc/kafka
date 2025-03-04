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

import org.apache.kafka.common.security.oauthbearer.GrantType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JwtBearerAccessTokenRetrieverTest extends OAuthBearerTest {

    private final Time time = new MockTime();

    @Test
    public void testFormatRequestBody() throws Exception {
        AssertionCreator assertionCreator = generateAssertionCreator(generateKeyPair().getPrivate());
        Map<String, Object> staticClaims = generateClaims();

        try (JwtBearerAccessTokenRetriever requestFormatter = generateRetriever(assertionCreator, staticClaims)) {
            String assertion = assertionCreator.create(staticClaims);
            String requestBody = requestFormatter.formatRequestBody();
            String expected = "grant_type=" + URLEncoder.encode(GrantType.JWT_BEARER.value(), StandardCharsets.UTF_8) + "&assertion=" + assertion;
            assertEquals(expected, requestBody);
        }
    }

    @Test
    public void testFormatRequestHeaders() throws Exception {
        AssertionCreator assertionCreator = generateAssertionCreator(generateKeyPair().getPrivate());
        Map<String, Object> staticClaims = generateClaims();

        try (JwtBearerAccessTokenRetriever requestFormatter = generateRetriever(assertionCreator, staticClaims)) {
            assertEquals(Collections.singletonMap("Content-Type", "application/x-www-form-urlencoded"), requestFormatter.formatRequestHeaders());
        }
    }

    private KeyPair generateKeyPair() {
        try {
            KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
            keyGen.initialize(2048);
            return keyGen.generateKeyPair();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Received unexpected error during private key generation", e);
        }
    }

    private Map<String, Object> generateClaims() {
        Map<String, Object> claims = new HashMap<>();
        claims.put("sub", "testTokenSubject");
        claims.put("iss", "testTokenIssuer");
        claims.put("aud", "testTokenAudience");
        return claims;
    }

    private AssertionCreator generateAssertionCreator(PrivateKey privateKey) {
        return new DefaultAssertionCreator(
            time,
            () -> Base64.getEncoder().encodeToString(privateKey.getEncoded()),
            "dummyPrivateKeyId",
            AssertionCreator.SigningAlgorithm.RS256.name()
        );
    }

    private JwtBearerAccessTokenRetriever generateRetriever(AssertionCreator assertionCreator,
                                                            Map<String, Object> staticClaims) {
        return new JwtBearerAccessTokenRetriever(
            assertionCreator,
            staticClaims,
            null,
            "https://www.example.com",
            100,
            10000,
            null,
            null
        );
    }

}