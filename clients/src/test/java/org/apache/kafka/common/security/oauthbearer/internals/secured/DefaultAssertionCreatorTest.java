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
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtConsumerBuilder;
import org.jose4j.jwt.consumer.JwtContext;
import org.jose4j.jwx.JsonWebStructure;
import org.jose4j.lang.InvalidAlgorithmException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DefaultAssertionCreatorTest extends OAuthBearerTest {

    @Test
    public void testPrivateKeyId() throws Exception {
        KeyPair keyPair = generateKeyPair();
        Builder builder = new Builder(keyPair.getPrivate())
            .setPrivateKeyId("test-id");

        Map<String, Object> claims = generateClaims();
        AssertionCreator assertionCreator = builder.build();
        String assertion = assertionCreator.create(claims);
        JwtConsumer jwtConsumer = jwtConsumer(keyPair.getPublic(), claims);
        JwtContext context = jwtConsumer.process(assertion);
        List<JsonWebStructure> joseObjects = context.getJoseObjects();
        assertNotNull(joseObjects);
        assertEquals(1, joseObjects.size());
        JsonWebStructure jsonWebStructure = joseObjects.get(0);
        assertEquals("test-id", jsonWebStructure.getKeyIdHeaderValue());
    }

    @Test
    public void testPrivateKeySecret() throws Exception {
        KeyPair keyPair = generateKeyPair();
        Builder builder = new Builder(keyPair.getPrivate());
        AssertionCreator assertionCreator = builder.build();
        Map<String, Object> claims = generateClaims();
        String assertion = assertionCreator.create(claims);
        JwtConsumer jwtConsumer = jwtConsumer(keyPair.getPublic(), claims);
        jwtConsumer.process(assertion);
    }

    @Test
    public void testInvalidPrivateKeySecret() {
        // Intentionally "mangle" the private key secret by stripping off the first character.
        Supplier<String> privateKeySupplier = () -> generatePrivateKeySecret().substring(1);

        AssertionCreator assertionCreator = new DefaultAssertionCreator(
            new MockTime(),
            privateKeySupplier,
            "foo",
            "RS256"
        );

        assertThrows(KafkaException.class, () -> assertionCreator.create(generateClaims()));
    }

    @ParameterizedTest
    @CsvSource("RS256,ES256")
    public void testTokenSigningAlgo(String tokenSigningAlgo) throws Exception {
        KeyPair keyPair = generateKeyPair();
        Builder builder = new Builder(keyPair.getPrivate())
            .setPrivateKeySigningAlgorithm(tokenSigningAlgo);

        AssertionCreator assertionCreator = builder.build();
        Map<String, Object> claims = generateClaims();
        String assertion = assertionCreator.create(claims);
        JwtConsumer jwtConsumer = jwtConsumer(keyPair.getPublic(), claims);
        JwtContext context = jwtConsumer.process(assertion);
        List<JsonWebStructure> joseObjects = context.getJoseObjects();
        assertNotNull(joseObjects);
        assertEquals(1, joseObjects.size());
        JsonWebStructure jsonWebStructure = joseObjects.get(0);
        assertEquals(tokenSigningAlgo, jsonWebStructure.getAlgorithmHeaderValue());
    }

    @Test
    public void testInvalidTokenSigningAlgo() {
        PrivateKey privateKey = generateKeyPair().getPrivate();
        Builder builder = new Builder(privateKey)
            .setPrivateKeySigningAlgorithm("thisisnotvalid");
        AssertionCreator assertionCreator = builder.build();
        Map<String, Object> claims = generateClaims();
        Exception e = assertThrows(KafkaException.class, () -> assertionCreator.create(claims));
        assertNotNull(e);
        Throwable cause = e.getCause();
        assertNotNull(cause);
        assertInstanceOf(InvalidAlgorithmException.class, cause);
    }

    private JwtConsumer jwtConsumer(PublicKey publicKey, Map<String, Object> expectedValues) {
        JwtConsumerBuilder builder = new JwtConsumerBuilder()
            .setVerificationKey(publicKey)
            .setRequireExpirationTime()
            .setAllowedClockSkewInSeconds(30);

        if (expectedValues.containsKey("sub"))
            builder = builder.setExpectedSubject(expectedValues.get("sub").toString());

        if (expectedValues.containsKey("iss"))
            builder = builder.setExpectedIssuer(expectedValues.get("iss").toString());

        if (expectedValues.containsKey("aud"))
            builder = builder.setExpectedAudience(expectedValues.get("aud").toString());

        return builder.build();
    }

    private String generatePrivateKeySecret(PrivateKey privateKey) {
        return Base64.getEncoder().encodeToString(privateKey.getEncoded());
    }

    private String generatePrivateKeySecret() {
        return generatePrivateKeySecret(generateKeyPair().getPrivate());
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

    private static class Builder {

        private final Time time = new MockTime();
        private final PrivateKey privateKey;
        private String privateKeyId = "testPrivateKeyId";
        private String privateKeySigningAlgorithm = "RS256";

        public Builder(PrivateKey privateKey) {
            this.privateKey = privateKey;
        }

        public Builder setPrivateKeyId(String privateKeyId) {
            this.privateKeyId = privateKeyId;
            return this;
        }

        public Builder setPrivateKeySigningAlgorithm(String privateKeySigningAlgorithm) {
            this.privateKeySigningAlgorithm = privateKeySigningAlgorithm;
            return this;
        }


        private AssertionCreator build() {
            Supplier<String> privateKeySupplier = () -> Base64.getEncoder().encodeToString(privateKey.getEncoded());

            return new DefaultAssertionCreator(
                time,
                privateKeySupplier,
                privateKeyId,
                privateKeySigningAlgorithm
            );
        }
    }
}