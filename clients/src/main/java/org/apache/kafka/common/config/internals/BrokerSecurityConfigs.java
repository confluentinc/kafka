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
package org.apache.kafka.common.config.internals;

import org.apache.kafka.common.config.SaslConfigs;

import java.util.Collections;
import java.util.List;

/**
 * Common home for broker-side security configs which need to be accessible from the libraries shared
 * between the broker and the client.
 *
 * Note this is an internal API and subject to change without notice.
 */
public class BrokerSecurityConfigs {

    public static final String PRINCIPAL_BUILDER_CLASS_CONFIG = "principal.builder.class";
    public static final String SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_CONFIG = "sasl.kerberos.principal.to.local.rules";
    public static final String SSL_CLIENT_AUTH_CONFIG = "ssl.client.auth";
    public static final String SASL_ENABLED_MECHANISMS_CONFIG = "sasl.enabled.mechanisms";
    public static final String SASL_SERVER_CALLBACK_HANDLER_CLASS = "sasl.server.callback.handler.class";
    public static final String SSL_PRINCIPAL_MAPPING_RULES_CONFIG = "ssl.principal.mapping.rules";
    public static final String CONNECTIONS_MAX_REAUTH_MS = "connections.max.reauth.ms";
    public static final int DEFAULT_SASL_SERVER_MAX_RECEIVE_SIZE = 524288;
    public static final String SASL_SERVER_MAX_RECEIVE_SIZE_CONFIG = "sasl.server.max.receive.size";
    public static final String SSL_ALLOW_DN_CHANGES_CONFIG = "ssl.allow.dn.changes";
    public static final boolean DEFAULT_SSL_ALLOW_DN_CHANGES_VALUE = false;
    public static final String SSL_ALLOW_SAN_CHANGES_CONFIG = "ssl.allow.san.changes";
    public static final boolean DEFAULT_SSL_ALLOW_SAN_CHANGES_VALUE = false;

    public static final String PRINCIPAL_BUILDER_CLASS_DOC = "The fully qualified name of a class that implements the " +
            "KafkaPrincipalBuilder interface, which is used to build the KafkaPrincipal object used during " +
            "authorization. If no principal builder is defined, the default behavior depends " +
            "on the security protocol in use. For SSL authentication,  the principal will be derived using the " +
            "rules defined by <code>" + SSL_PRINCIPAL_MAPPING_RULES_CONFIG + "</code> applied on the distinguished " +
            "name from the client certificate if one is provided; otherwise, if client authentication is not required, " +
            "the principal name will be ANONYMOUS. For SASL authentication, the principal will be derived using the " +
            "rules defined by <code>" + SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_CONFIG + "</code> if GSSAPI is in use, " +
            "and the SASL authentication ID for other mechanisms. For PLAINTEXT, the principal will be ANONYMOUS.";

    public static final String SSL_PRINCIPAL_MAPPING_RULES_DOC = "A list of rules for mapping from distinguished name" +
            " from the client certificate to short name. The rules are evaluated in order and the first rule that matches" +
            " a principal name is used to map it to a short name. Any later rules in the list are ignored. By default," +
            " distinguished name of the X.500 certificate will be the principal. For more details on the format please" +
            " see <a href=\"#security_authz\"> security authorization and acls</a>. Note that this configuration is ignored" +
            " if an extension of KafkaPrincipalBuilder is provided by the <code>" + PRINCIPAL_BUILDER_CLASS_CONFIG + "</code>" +
           " configuration.";
    public static final String DEFAULT_SSL_PRINCIPAL_MAPPING_RULES = "DEFAULT";

    public static final String SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_DOC = "A list of rules for mapping from principal " +
            "names to short names (typically operating system usernames). The rules are evaluated in order and the " +
            "first rule that matches a principal name is used to map it to a short name. Any later rules in the list are " +
            "ignored. By default, principal names of the form <code>{username}/{hostname}@{REALM}</code> are mapped " +
            "to <code>{username}</code>. For more details on the format please see <a href=\"#security_authz\"> " +
            "security authorization and acls</a>. Note that this configuration is ignored if an extension of " +
            "<code>KafkaPrincipalBuilder</code> is provided by the <code>" + PRINCIPAL_BUILDER_CLASS_CONFIG + "</code> configuration.";
    public static final List<String> DEFAULT_SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES = Collections.singletonList("DEFAULT");

    public static final String SSL_CLIENT_AUTH_DOC = "Configures kafka broker to request client authentication."
            + " The following settings are common: "
            + " <ul>"
            + " <li><code>ssl.client.auth=required</code> If set to required client authentication is required."
            + " <li><code>ssl.client.auth=requested</code> This means client authentication is optional."
            + " unlike required, if this option is set client can choose not to provide authentication information about itself"
            + " <li><code>ssl.client.auth=none</code> This means client authentication is not needed."
            + "</ul>";

    public static final String SASL_ENABLED_MECHANISMS_DOC = "The list of SASL mechanisms enabled in the Kafka server. "
            + "The list may contain any mechanism for which a security provider is available. "
            + "Only GSSAPI is enabled by default.";
    public static final List<String> DEFAULT_SASL_ENABLED_MECHANISMS = Collections.singletonList(SaslConfigs.GSSAPI_MECHANISM);

    public static final String SASL_SERVER_CALLBACK_HANDLER_CLASS_DOC = "The fully qualified name of a SASL server callback handler "
            + "class that implements the AuthenticateCallbackHandler interface. Server callback handlers must be prefixed with "
            + "listener prefix and SASL mechanism name in lower-case. For example, "
            + "listener.name.sasl_ssl.plain.sasl.server.callback.handler.class=com.example.CustomPlainCallbackHandler.";

    public static final String CONNECTIONS_MAX_REAUTH_MS_DOC = "When explicitly set to a positive number (the default is 0, not a positive number), "
            + "a session lifetime that will not exceed the configured value will be communicated to v2.2.0 or later clients when they authenticate. "
            + "The broker will disconnect any such connection that is not re-authenticated within the session lifetime and that is then subsequently "
            + "used for any purpose other than re-authentication. Configuration names can optionally be prefixed with listener prefix and SASL "
            + "mechanism name in lower-case. For example, listener.name.sasl_ssl.oauthbearer.connections.max.reauth.ms=3600000";

    public static final String SASL_SERVER_MAX_RECEIVE_SIZE_DOC = "The maximum receive size allowed before and during initial SASL authentication." +
            " Default receive size is 512KB. GSSAPI limits requests to 64K, but we allow upto 512KB by default for custom SASL mechanisms. In practice," +
            " PLAIN, SCRAM and OAUTH mechanisms can use much smaller limits.";

    public static final String SSL_ALLOW_DN_CHANGES_DOC = "Indicates whether changes to the certificate distinguished name should be allowed during" +
            " a dynamic reconfiguration of certificates or not.";

    public static final String SSL_ALLOW_SAN_CHANGES_DOC = "Indicates whether changes to the certificate subject alternative names should be allowed during " +
            "a dynamic reconfiguration of certificates or not.";

    // The allowlist of the SASL OAUTHBEARER endpoints
    public static final String ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG = "org.apache.kafka.sasl.oauthbearer.allowed.urls";

}
