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

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenBuilder;
import org.apache.kafka.common.security.oauthbearer.internals.secured.CloseableVerificationKeyResolver;
import org.apache.kafka.common.utils.LogCaptureAppender;

import org.apache.logging.log4j.Level;
import org.jose4j.jwk.PublicJsonWebKey;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.lang.InvalidAlgorithmException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_EXPECTED_AUDIENCE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_EXPECTED_ISSUER;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BrokerJwtValidatorTest extends JwtValidatorTest {

    private static final String ATTACKER_ISSUER = "https://evil.example/attacker";

    private static final String EXPECTED_ISSUER = "https://idp.legit.example/";

    @Override
    protected JwtValidator createJwtValidator(AccessTokenBuilder builder) {
        CloseableVerificationKeyResolver resolver = (jws, nestingContext) -> builder.jwk().getKey();
        return new BrokerJwtValidator(resolver);
    }

    @Test
    public void testRsaEncryptionAlgorithm() throws Exception {
        PublicJsonWebKey jwk = createRsaJwk();
        testEncryptionAlgorithm(jwk, AlgorithmIdentifiers.RSA_USING_SHA256);
    }

    @Test
    public void testEcdsaEncryptionAlgorithm() throws Exception {
        PublicJsonWebKey jwk = createEcJwk();
        testEncryptionAlgorithm(jwk, AlgorithmIdentifiers.ECDSA_USING_P256_CURVE_AND_SHA256);
    }

    @Test
    public void testInvalidEncryptionAlgorithm() throws Exception {
        PublicJsonWebKey jwk = createRsaJwk();

        assertThrowsWithMessage(InvalidAlgorithmException.class,
            () -> testEncryptionAlgorithm(jwk, "fake"),
            "fake is an unknown, unsupported or unavailable alg algorithm");
    }

    @Test
    public void testMissingSubShouldBeValid() throws Exception {
        String subClaimName = "client_id";
        String subject = "otherSub";
        PublicJsonWebKey jwk = createRsaJwk();
        AccessTokenBuilder tokenBuilder = new AccessTokenBuilder()
            .jwk(jwk)
            .alg(AlgorithmIdentifiers.RSA_USING_SHA256)
            .addCustomClaim(subClaimName, subject)
            .addCustomClaim("iss", EXPECTED_ISSUER)
            .subjectClaimName(subClaimName)
            .subject(null);
        JwtValidator validator = createJwtValidator(tokenBuilder);
        Map<String, ?> saslConfigs = getSaslConfigs(Map.of(
            SaslConfigs.SASL_OAUTHBEARER_SUB_CLAIM_NAME, subClaimName,
            SASL_OAUTHBEARER_EXPECTED_ISSUER, EXPECTED_ISSUER));
        validator.configure(saslConfigs, OAUTHBEARER_MECHANISM, getJaasConfigEntries());

        // Validation should succeed (e.g. signature verification) even if sub claim is missing
        OAuthBearerToken token = validator.validate(tokenBuilder.build());

        assertEquals(subject, token.principalName());
    }

    @Test
    public void testExpectedIssuerMismatch() throws Exception {
        PublicJsonWebKey jwk = createRsaJwk();
        AccessTokenBuilder builder = new AccessTokenBuilder()
                .jwk(jwk)
                .alg(AlgorithmIdentifiers.RSA_USING_SHA256)
                .addCustomClaim("iss", ATTACKER_ISSUER);
        String accessToken = builder.build();

        CloseableVerificationKeyResolver resolver = (jws, nestingContext) -> jwk.getKey();
        BrokerJwtValidator validator = new BrokerJwtValidator(resolver);
        Map<String, ?> saslConfigs = getSaslConfigs(
                SASL_OAUTHBEARER_EXPECTED_ISSUER, EXPECTED_ISSUER);
        validator.configure(saslConfigs, OAUTHBEARER_MECHANISM, getJaasConfigEntries());

        JwtValidatorException e = assertThrows(JwtValidatorException.class,
                () -> validator.validate(accessToken));
        assertErrorMessageContains(e.getMessage(), ATTACKER_ISSUER);
    }

    @Test
    public void testWarnsWhenExpectedIssuerUnset() throws Exception {
        PublicJsonWebKey jwk = createRsaJwk();
        CloseableVerificationKeyResolver resolver = (jws, nestingContext) -> jwk.getKey();
        BrokerJwtValidator validator = new BrokerJwtValidator(resolver);

        try (LogCaptureAppender appender = LogCaptureAppender.createAndRegister()) {
            appender.setClassLogger(BrokerJwtValidator.class, Level.WARN);
            validator.configure(getSaslConfigs(), OAUTHBEARER_MECHANISM, getJaasConfigEntries());

            List<String> warnings = appender.getMessages("WARN");
            assertTrue(warnings.stream().anyMatch(message -> message.contains(SASL_OAUTHBEARER_EXPECTED_ISSUER)),
                    "Expected a WARN log mentioning " + SASL_OAUTHBEARER_EXPECTED_ISSUER + ", but got: " + warnings);
        }
    }

    @Test
    public void testWarnsWhenExpectedAudienceUnset() throws Exception {
        PublicJsonWebKey jwk = createRsaJwk();
        CloseableVerificationKeyResolver resolver = (jws, nestingContext) -> jwk.getKey();
        BrokerJwtValidator validator = new BrokerJwtValidator(resolver);

        try (LogCaptureAppender appender = LogCaptureAppender.createAndRegister()) {
            appender.setClassLogger(BrokerJwtValidator.class, Level.WARN);
            validator.configure(getSaslConfigs(), OAUTHBEARER_MECHANISM, getJaasConfigEntries());

            List<String> warnings = appender.getMessages("WARN");
            assertTrue(warnings.stream().anyMatch(message -> message.contains(SASL_OAUTHBEARER_EXPECTED_AUDIENCE)),
                    "Expected a WARN log mentioning " + SASL_OAUTHBEARER_EXPECTED_AUDIENCE + ", but got: " + warnings);
        }
    }

    private void testEncryptionAlgorithm(PublicJsonWebKey jwk, String alg) throws Exception {
        AccessTokenBuilder builder = new AccessTokenBuilder().jwk(jwk).alg(alg).addCustomClaim("iss", EXPECTED_ISSUER);
        JwtValidator validator = createJwtValidator(builder);
        validator.configure(getSaslConfigs(SASL_OAUTHBEARER_EXPECTED_ISSUER, EXPECTED_ISSUER),
                OAUTHBEARER_MECHANISM, getJaasConfigEntries());
        String accessToken = builder.build();
        OAuthBearerToken token = validator.validate(accessToken);

        assertEquals(builder.subject(), token.principalName());
        assertEquals(builder.issuedAtSeconds() * 1000, token.startTimeMs());
        assertEquals(builder.expirationSeconds() * 1000, token.lifetimeMs());
        assertEquals(1, token.scope().size());
    }

}
