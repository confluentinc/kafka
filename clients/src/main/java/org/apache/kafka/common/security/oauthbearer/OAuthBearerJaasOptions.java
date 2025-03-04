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

    public static final String GRANT_TYPE = "grantType";

    public static final String CLIENT_ID = "clientId";

    public static final String CLIENT_SECRET = "clientSecret";

    public static final String SCOPE = "scope";

    private static final String JWT_BEARER_PREFIX = "jwt-bearer.";

    public static final String JWT_BEARER_PRIVATE_KEY_ID = JWT_BEARER_PREFIX + "privateKeyId";

    public static final String JWT_BEARER_PRIVATE_KEY_FILE_NAME = JWT_BEARER_PREFIX + "privateKeyFileName";

    public static final String JWT_BEARER_PRIVATE_KEY_ALGORITHM = JWT_BEARER_PREFIX + "privateKeyAlgorithm";

    public static final String JWT_BEARER_CLAIM_PREFIX = JWT_BEARER_PREFIX + "claim.";

    private OAuthBearerJaasOptions() {
        // Intentionally empty to prevent instantiation.
    }
}
