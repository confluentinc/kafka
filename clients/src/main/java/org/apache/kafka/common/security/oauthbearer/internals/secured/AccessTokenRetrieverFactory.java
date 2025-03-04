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
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import javax.net.ssl.SSLSocketFactory;

import static org.apache.kafka.common.config.SaslConfigs.DEFAULT_SASL_OAUTHBEARER_HEADER_URLENCODE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_CONNECT_TIMEOUT_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_READ_TIMEOUT_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MAX_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_LOGIN_RETRY_BACKOFF_MS;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_HEADER_URLENCODE;
import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerJaasOptions.CLIENT_ID;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerJaasOptions.CLIENT_SECRET;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerJaasOptions.GRANT_TYPE;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerJaasOptions.JWT_BEARER_CLAIM_PREFIX;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerJaasOptions.JWT_BEARER_PRIVATE_KEY_ALGORITHM;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerJaasOptions.JWT_BEARER_PRIVATE_KEY_FILE_NAME;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerJaasOptions.JWT_BEARER_PRIVATE_KEY_ID;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerJaasOptions.SCOPE;

public class AccessTokenRetrieverFactory  {

    /**
     * Create an {@link AccessTokenRetriever} from the given SASL and JAAS configuration.
     *
     * <b>Note</b>: the returned <code>AccessTokenRetriever</code> is <em>not</em> initialized
     * here and must be done by the caller prior to use.
     *
     * @param time       Time
     * @param configs    SASL configuration
     * @param jaasConfig JAAS configuration
     *
     * @return Non-<code>null</code> {@link AccessTokenRetriever}
     */

    public static AccessTokenRetriever create(Time time, Map<String, ?> configs, Map<String, Object> jaasConfig) {
        return create(time, configs, null, jaasConfig);
    }

    public static AccessTokenRetriever create(Time time,
        Map<String, ?> configs,
        String saslMechanism,
        Map<String, Object> jaasConfig) {
        ConfigurationUtils cu = new ConfigurationUtils(configs, saslMechanism);
        URL tokenEndpointUrl = cu.validateUrl(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL);

        if (tokenEndpointUrl.getProtocol().toLowerCase(Locale.ROOT).equals("file")) {
            return new FileTokenRetriever(cu.validateFile(SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL));
        } else {
            JaasOptionsUtils jou = new JaasOptionsUtils(jaasConfig);
            SSLSocketFactory sslSocketFactory = null;

            if (jou.shouldCreateSSLSocketFactory(tokenEndpointUrl))
                sslSocketFactory = jou.createSSLSocketFactory();

            String grantType = Optional
                .ofNullable(cu.validateString(GRANT_TYPE, false))
                .orElse(ClientCredentialsAccessTokenRetriever.GRANT_TYPE);

            if (grantType.equalsIgnoreCase(ClientCredentialsAccessTokenRetriever.GRANT_TYPE)) {
                String clientId = jou.validateString(CLIENT_ID);
                String clientSecret = jou.validateString(CLIENT_SECRET);
                String scope = jou.validateString(SCOPE, false);
                boolean urlencodeHeader = validateUrlencodeHeader(cu);

                return new ClientCredentialsAccessTokenRetriever(clientId,
                    clientSecret,
                    scope,
                    sslSocketFactory,
                    tokenEndpointUrl.toString(),
                    cu.validateLong(SASL_LOGIN_RETRY_BACKOFF_MS),
                    cu.validateLong(SASL_LOGIN_RETRY_BACKOFF_MAX_MS),
                    cu.validateInteger(SASL_LOGIN_CONNECT_TIMEOUT_MS, false),
                    cu.validateInteger(SASL_LOGIN_READ_TIMEOUT_MS, false),
                    urlencodeHeader);
            } else if (grantType.equalsIgnoreCase(JwtBearerAccessTokenRetriever.GRANT_TYPE)) {
                String privateKeyId = jou.validateString(JWT_BEARER_PRIVATE_KEY_ID);
                Path privateKeyFileName = jou.validateFile(JWT_BEARER_PRIVATE_KEY_FILE_NAME);
                String privateKeySigningAlgorithm = jou.validateString(JWT_BEARER_PRIVATE_KEY_ALGORITHM);
                Map<String, Object> staticClaims = getStaticClaims(jaasConfig);

                Supplier<String> privateKeySupplier = () -> {
                    String fileName = privateKeyFileName.toFile().getAbsolutePath();

                    try {
                        return Utils.readFileAsString(fileName);
                    } catch (IOException e) {
                        throw new KafkaException("Could not read the private key from the file " + fileName, e);
                    }
                };

                AssertionCreator assertionCreator = new DefaultAssertionCreator(
                    time,
                    privateKeySupplier,
                    privateKeyId,
                    privateKeySigningAlgorithm
                );

                return new JwtBearerAccessTokenRetriever(
                    assertionCreator,
                    staticClaims,
                    sslSocketFactory,
                    tokenEndpointUrl.toString(),
                    cu.validateLong(SASL_LOGIN_RETRY_BACKOFF_MS),
                    cu.validateLong(SASL_LOGIN_RETRY_BACKOFF_MAX_MS),
                    cu.validateInteger(SASL_LOGIN_CONNECT_TIMEOUT_MS, false),
                    cu.validateInteger(SASL_LOGIN_READ_TIMEOUT_MS, false)
                );
            } else {
                throw new KafkaException("Unsupported grant type provided: " + grantType);
            }
        }
    }

    /**
     * In some cases, the incoming {@link Map} doesn't contain a value for
     * {@link SaslConfigs#SASL_OAUTHBEARER_HEADER_URLENCODE}. Returning {@code null} from {@link Map#get(Object)}
     * will cause a {@link NullPointerException} when it is later unboxed.
     *
     * <p/>
     *
     * This utility method ensures that we have a non-{@code null} value to use in the
     * {@link HttpAccessTokenRetriever} constructor.
     */
    static boolean validateUrlencodeHeader(ConfigurationUtils configurationUtils) {
        Boolean urlencodeHeader = configurationUtils.validateBoolean(SASL_OAUTHBEARER_HEADER_URLENCODE, false);

        if (urlencodeHeader != null)
            return urlencodeHeader;
        else
            return DEFAULT_SASL_OAUTHBEARER_HEADER_URLENCODE;
    }

    /**
     * Support for static claims allows the client to pass arbitrary claims to the identity provider.
     */
    static Map<String, Object> getStaticClaims(Map<String, Object> jaasConfig) {
        Map<String, Object> claims = new HashMap<>();

        jaasConfig.forEach((k, v) -> {
            if (k.startsWith(JWT_BEARER_CLAIM_PREFIX)) {
                String claimName = k.substring(JWT_BEARER_CLAIM_PREFIX.length());
                String claimValue = String.valueOf(v);
                claims.put(claimName, claimValue);
            }
        });

        return claims;
    }
}