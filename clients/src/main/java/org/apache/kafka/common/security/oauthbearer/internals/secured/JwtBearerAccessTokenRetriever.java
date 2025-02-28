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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.GrantType;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * <code>ClientCredentialsAccessTokenRetriever</code> is an {@link HttpAccessTokenRetriever} that will
 * post an assertion using the jwt-bearer grant type to a publicized token endpoint URL
 * ({@link SaslConfigs#SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL}).
 */

public class JwtBearerAccessTokenRetriever extends HttpAccessTokenRetriever {

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

    private final Time time;
    private final String privateKeyId;
    private final String privateKeySecret;
    private final SigningAlgorithm privateKeySigningAlgorithm;
    private final String subject;
    private final String issuer;
    private final String audience;
    private final Map<String, String> supplementaryClaims;

    @SuppressWarnings("ParameterNumber")
    public JwtBearerAccessTokenRetriever(Time time,
                                         String privateKeyId,
                                         String privateKeySecret,
                                         String privateKeySigningAlgorithm,
                                         String subject,
                                         String issuer,
                                         String audience,
                                         Map<String, String> supplementaryClaims,
                                         SSLSocketFactory sslSocketFactory,
                                         String tokenEndpointUrl,
                                         long loginRetryBackoffMs,
                                         long loginRetryBackoffMaxMs,
                                         Integer loginConnectTimeoutMs,
                                         Integer loginReadTimeoutMs) {
        super(
            sslSocketFactory,
            tokenEndpointUrl,
            loginRetryBackoffMs,
            loginRetryBackoffMaxMs,
            loginConnectTimeoutMs,
            loginReadTimeoutMs
        );
        this.time = time;
        this.privateKeyId = privateKeyId;
        this.privateKeySecret = privateKeySecret;
        this.privateKeySigningAlgorithm = SigningAlgorithm.fromName(privateKeySigningAlgorithm);
        this.subject = subject;
        this.issuer = issuer;
        this.audience = audience;
        this.supplementaryClaims = supplementaryClaims;
    }

    @Override
    protected String formatRequestBody() {
        String assertion;

        try {
            assertion = createAssertion();
        } catch (Exception e) {
            throw new KafkaException("Error signing OAuth assertion", e);
        }

        String encodedGrantType = URLEncoder.encode(GrantType.JWT_BEARER.value(), StandardCharsets.UTF_8);
        String encodedAssertion = URLEncoder.encode(assertion, StandardCharsets.UTF_8);
        return String.format("grant_type=%s&assertion=%s", encodedGrantType, encodedAssertion);
    }

    @Override
    protected Map<String, String> formatRequestHeaders() {
        return Collections.singletonMap("Content-Type", "application/x-www-form-urlencoded");
    }

    String createAssertion() throws IOException, GeneralSecurityException {
        ObjectMapper mapper = new ObjectMapper();
        Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();
        String header = encodeHeader(mapper, encoder);
        String payload = encodePayload(mapper, encoder);
        String content = header + "." + payload;
        PrivateKey privateKey = getPrivateKey();
        String signedContent = sign(privateKey, content);
        return content + "." + signedContent;
    }

    PrivateKey getPrivateKey() throws NoSuchAlgorithmException, InvalidKeySpecException {
        byte[] pkcs8EncodedBytes = Base64.getDecoder().decode(privateKeySecret);
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(pkcs8EncodedBytes);
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        return keyFactory.generatePrivate(keySpec);
    }

    String encodeHeader(ObjectMapper mapper, Base64.Encoder encoder) throws IOException {
        Map<String, Object> values = new HashMap<>();
        values.put("alg", privateKeySigningAlgorithm);
        values.put("typ", "JWT");
        values.put("kid", privateKeyId);

        String json = mapper.writeValueAsString(values);
        return encoder.encodeToString(Utils.utf8(json));
    }

    String encodePayload(ObjectMapper mapper, Base64.Encoder encoder) throws IOException {
        long currentTimeSecs = time.milliseconds() / 1000L;
        long expirationSecs = currentTimeSecs + Duration.ofMinutes(60).toSeconds();

        Map<String, Object> values = new HashMap<>();
        values.put("iss", issuer);
        values.put("sub", subject);
        values.put("aud", audience);
        values.put("iat", currentTimeSecs);
        values.put("exp", expirationSecs);
        values.putAll(supplementaryClaims);

        String json = mapper.writeValueAsString(values);
        return encoder.encodeToString(Utils.utf8(json));
    }

    String sign(PrivateKey privateKey, String contentToSign) throws InvalidKeyException, SignatureException, NoSuchAlgorithmException {
        Signature signature = privateKeySigningAlgorithm.newSignature();
        signature.initSign(privateKey);
        signature.update(contentToSign.getBytes(StandardCharsets.UTF_8));
        byte[] signedContent = signature.sign();
        return Base64.getUrlEncoder().withoutPadding().encodeToString(signedContent);
    }
}
