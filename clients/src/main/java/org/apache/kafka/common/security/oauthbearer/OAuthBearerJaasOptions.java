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

import static org.apache.kafka.common.config.SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL;

public class OAuthBearerJaasOptions {

    public static final String CLIENT_ID = "clientId";
    public static final String CLIENT_SECRET = "clientSecret";
    public static final String SCOPE = "scope";

    public static final String CLIENT_ID_DOC = "The OAuth/OIDC identity provider-issued " +
        "client ID to uniquely identify the service account to use for authentication for " +
        "this client. The value must be paired with a corresponding " + CLIENT_SECRET + " " +
        "value and is provided to the OAuth provider using the OAuth " +
        "clientcredentials grant type.";

    public static final String CLIENT_SECRET_DOC = "The OAuth/OIDC identity provider-issued " +
        "client secret serves a similar function as a password to the " + CLIENT_ID + " " +
        "account and identifies the service account to use for authentication for " +
        "this client. The value must be paired with a corresponding " + CLIENT_ID + " " +
        "value and is provided to the OAuth provider using the OAuth " +
        "clientcredentials grant type.";

    public static final String SCOPE_DOC = "The (optional) HTTP/HTTPS login request to the " +
        "token endpoint (" + SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL + ") may need to specify an " +
        "OAuth \"scope\". If so, the " + SCOPE + " is used to provide the value to " +
        "include with the login request.";

    private static final String JWT_BEARER_PREFIX = "jwt-bearer.";
    public static final String JWT_BEARER_PRIVATE_KEY_ID = JWT_BEARER_PREFIX + "privateKeyId";
    public static final String JWT_BEARER_PRIVATE_KEY_SECRET = JWT_BEARER_PREFIX + "privateKeySecret";
    public static final String JWT_BEARER_PRIVATE_KEY_SIGNING_ALGORITHM = JWT_BEARER_PREFIX + "privateKeyAlgorithm";
    public static final String JWT_BEARER_SUBJECT = JWT_BEARER_PREFIX + "subject";
    public static final String JWT_BEARER_ISSUER = JWT_BEARER_PREFIX + "issuer";
    public static final String JWT_BEARER_AUDIENCE = JWT_BEARER_PREFIX + "audience";
    public static final String JWT_BEARER_CLAIM_PREFIX = JWT_BEARER_PREFIX + "claim.";

    public static final String JWT_BEARER_PRIVATE_KEY_ID_DOC = "The ID for the key used to decrypt the token. Used in the 'kid' JWT header.";
    public static final String JWT_BEARER_PRIVATE_KEY_SECRET_DOC = "The private key used to sign the JWT token sent " +
        "to the token endpoint. This must be in PEM format without the header and footer.";
    public static final String JWT_BEARER_PRIVATE_KEY_SIGNING_ALGORITHM_DOC = "The algorithm used to sign the " +
        "JWT token sent to the token endpoint.";
    public static final String JWT_BEARER_SUBJECT_DOC = "The subject of the JWT token sent to the token endpoint.";
    public static final String JWT_BEARER_ISSUER_DOC = "The issuer of the JWT token sent to the token endpoint.";
    public static final String JWT_BEARER_AUDIENCE_DOC = "The audience of the JWT token sent to the token endpoint.";
    public static final String JWT_BEARER_CLAIM_PREFIX_DOC = "";

    private OAuthBearerJaasOptions() {
        // Intentionally empty
    }
}
