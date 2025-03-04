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

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.GrantType;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerJaasOptions;

import javax.net.ssl.SSLSocketFactory;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

/**
 * <code>ClientCredentialsAccessTokenRetriever</code> is an {@link HttpAccessTokenRetriever} that will
 * post an assertion using the jwt-bearer grant type to a publicized token endpoint URL
 * ({@link SaslConfigs#SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL}).
 *
 * @see HttpAccessTokenRetriever
 * @see OAuthBearerJaasOptions#JWT_BEARER_PRIVATE_KEY_ID
 * @see OAuthBearerJaasOptions#JWT_BEARER_PRIVATE_KEY_SECRET
 * @see OAuthBearerJaasOptions#JWT_BEARER_PRIVATE_KEY_ALGORITHM
 * @see OAuthBearerJaasOptions#JWT_BEARER_CLAIM_PREFIX
 * @see SaslConfigs#SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL
 * @see SaslConfigs#SASL_OAUTHBEARER_GRANT_TYPE
 */
public class JwtBearerAccessTokenRetriever extends HttpAccessTokenRetriever {

    private final AssertionCreator assertionCreator;
    private final Map<String, Object> staticClaims;

    public JwtBearerAccessTokenRetriever(AssertionCreator assertionCreator,
                                         Map<String, Object> staticClaims,
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

        this.assertionCreator = assertionCreator;
        this.staticClaims = staticClaims;
    }

    @Override
    protected String formatRequestBody() {
        String assertion = assertionCreator.create(staticClaims);
        String encodedGrantType = URLEncoder.encode(GrantType.JWT_BEARER.value(), StandardCharsets.UTF_8);
        String encodedAssertion = URLEncoder.encode(assertion, StandardCharsets.UTF_8);
        return String.format("grant_type=%s&assertion=%s", encodedGrantType, encodedAssertion);
    }

    @Override
    protected Map<String, String> formatRequestHeaders() {
        return Collections.singletonMap("Content-Type", "application/x-www-form-urlencoded");
    }

}
