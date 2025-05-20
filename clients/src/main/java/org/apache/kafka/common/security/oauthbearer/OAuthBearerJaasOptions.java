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

/**
 * {@code OAuthBearerJaasOptions} holds the different options that can be configured by the user in the
 * {@link SaslConfigs#SASL_JAAS_CONFIG} configuration.
 */
public class OAuthBearerJaasOptions {

    /**
     * This provides a "namespace" for the jwt-bearer grant type-related JAAS options. The existing implementation
     * only supported the client_credentials grant type, so those do not have a prefix.
     */
    private static final String JWT_BEARER_PREFIX = "jwt-bearer.";

    /**
     * The {@code grantType} JAAS option is how the user specifies which OAuth grant type to use.
     */
    public static final String GRANT_TYPE = "grantType";

    /**
     * The scope value can be used by multiple grant types, so it has no prefix.
     */
    public static final String SCOPE = "scope";

    public static final String CLIENT_CREDENTIALS_CLIENT_ID = "clientId";

    public static final String CLIENT_CREDENTIALS_CLIENT_SECRET = "clientSecret";

    public static final String JWT_BEARER_PRIVATE_KEY_ID = JWT_BEARER_PREFIX + "privateKeyId";

    public static final String JWT_BEARER_PRIVATE_KEY_FILE_NAME = JWT_BEARER_PREFIX + "privateKeyFileName";

    public static final String JWT_BEARER_PRIVATE_KEY_ALGORITHM = JWT_BEARER_PREFIX + "privateKeyAlgorithm";

    /**
     * The jwt-bearer grant type requires a JWT be created on the client side, signed, and sent in the token
     * request. The JWT is built up from claims statically set in the JAAS options:
     *
     * <ul>
     *     <li>jwt-bearer.claim.sub=some-service-account</li>
     *     <li>jwt-bearer.claim.aud=my_audience</li>
     *     <li>jwt-bearer.claim.iss=https://example.com</li>
     * </ul>
     *
     * Those claims are used to create a JWT that looks like this:
     *
     * <pre>
     *    {
     *        "iat": 1741121401,
     *        "exp": 1741125001,
     *        "sub": "some-service-account",
     *        "aud": "my_audience",
     *        "iss": "https://example.com",
     *        "...": "...",
     *    }
     * </pre>
     *
     * <b>Note:</b> the JAAS format for specifying claims will likely change in the future to facilitate support for
     * arbitrarily complicated JWT JSON that includes integers, longs, booleans, lists, and maps.
     */
    public static final String JWT_BEARER_CLAIM_PREFIX = JWT_BEARER_PREFIX + "claim.";

    private OAuthBearerJaasOptions() {
        // Intentionally empty to prevent instantiation.
    }
}
