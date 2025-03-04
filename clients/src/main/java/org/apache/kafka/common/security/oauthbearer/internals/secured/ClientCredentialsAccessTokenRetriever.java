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
import org.apache.kafka.common.security.oauthbearer.OAuthBearerJaasOptions;
import org.apache.kafka.common.utils.Utils;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import javax.net.ssl.SSLSocketFactory;

/**
 * <code>ClientCredentialsAccessTokenRetriever</code> is an {@link HttpAccessTokenRetriever} that will
 * post client credentials
 * ({@link OAuthBearerJaasOptions#CLIENT_ID}/{@link OAuthBearerJaasOptions#CLIENT_SECRET})
 * to a publicized token endpoint URL
 * ({@link SaslConfigs#SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL}).
 *
 * @see HttpAccessTokenRetriever
 * @see OAuthBearerJaasOptions#CLIENT_ID
 * @see OAuthBearerJaasOptions#CLIENT_SECRET
 * @see OAuthBearerJaasOptions#SCOPE
 * @see SaslConfigs#SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL
 */

public class ClientCredentialsAccessTokenRetriever extends HttpAccessTokenRetriever {

    public static final String GRANT_TYPE = "client_credentials";

    private final String clientId;

    private final String clientSecret;

    private final String scope;

    private final boolean urlencodeHeader;

    public ClientCredentialsAccessTokenRetriever(String clientId,
                                                 String clientSecret,
                                                 String scope,
                                                 SSLSocketFactory sslSocketFactory,
                                                 String tokenEndpointUrl,
                                                 long loginRetryBackoffMs,
                                                 long loginRetryBackoffMaxMs,
                                                 Integer loginConnectTimeoutMs,
                                                 Integer loginReadTimeoutMs,
                                                 boolean urlencodeHeader) {
        super(
            sslSocketFactory,
            tokenEndpointUrl,
            loginRetryBackoffMs,
            loginRetryBackoffMaxMs,
            loginConnectTimeoutMs,
            loginReadTimeoutMs
        );
        this.clientId = Objects.requireNonNull(clientId);
        this.clientSecret = Objects.requireNonNull(clientSecret);
        this.scope = scope;
        this.urlencodeHeader = urlencodeHeader;
    }

    @Override
    protected String formatRequestBody() {
        return formatRequestBody(scope);
    }

    @Override
    protected Map<String, String> formatRequestHeaders() {
        return Collections.singletonMap(AUTHORIZATION_HEADER, formatAuthorizationHeader(clientId, clientSecret, urlencodeHeader));
    }

    static String formatAuthorizationHeader(String clientId, String clientSecret, boolean urlencode) {
        clientId = sanitizeString("the token endpoint request client ID parameter", clientId);
        clientSecret = sanitizeString("the token endpoint request client secret parameter", clientSecret);

        // according to RFC-6749 clientId & clientSecret must be urlencoded, see https://tools.ietf.org/html/rfc6749#section-2.3.1
        if (urlencode) {
            clientId = URLEncoder.encode(clientId, StandardCharsets.UTF_8);
            clientSecret = URLEncoder.encode(clientSecret, StandardCharsets.UTF_8);
        }

        String s = String.format("%s:%s", clientId, clientSecret);
        // Per RFC-7617, we need to use the *non-URL safe* base64 encoder. See KAFKA-14496.
        String encoded = Base64.getEncoder().encodeToString(Utils.utf8(s));
        return String.format("Basic %s", encoded);
    }

    static String formatRequestBody(String scope) {
        StringBuilder requestParameters = new StringBuilder();
        requestParameters.append("grant_type=" + GRANT_TYPE);

        if (!Utils.isBlank(scope)) {
            String encodedScope = URLEncoder.encode(scope.trim(), StandardCharsets.UTF_8);
            requestParameters.append("&scope=").append(encodedScope);
        }

        return requestParameters.toString();
    }

}
