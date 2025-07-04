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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.network.ConnectionMode;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.ssl.DefaultSslEngineFactory;
import org.apache.kafka.common.security.ssl.SslFactory;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.net.ssl.SSLSocketFactory;
import javax.security.auth.login.AppConfigurationEntry;

/**
 * <code>JaasOptionsUtils</code> is a utility class to perform logic for the JAAS options and
 * is separated out here for easier, more direct testing.
 */

public class JaasOptionsUtils {

    private static final Logger log = LoggerFactory.getLogger(JaasOptionsUtils.class);

    private final Map<String, Object> options;

    public JaasOptionsUtils(Map<String, Object> options) {
        this.options = options;
    }

    public JaasOptionsUtils(String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        this.options = getOptions(saslMechanism, jaasConfigEntries);
    }

    public static Map<String, Object> getOptions(String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        validateOAuthMechanismAndNonNullJaasConfig(saslMechanism, jaasConfigEntries);
        return Collections.unmodifiableMap(jaasConfigEntries.get(0).getOptions());
    }

    public static void validateOAuthMechanismAndNonNullJaasConfig(
            String saslMechanism,
            List<AppConfigurationEntry> jaasConfigEntries
    ) {
        if (!OAuthBearerLoginModule.OAUTHBEARER_MECHANISM.equals(saslMechanism)) {
            throw new IllegalArgumentException(String.format("Unexpected SASL mechanism: %s", saslMechanism));
        }

        if (Objects.requireNonNull(jaasConfigEntries).size() != 1 || jaasConfigEntries.get(0) == null) {
            throw new IllegalArgumentException(
                    String.format(
                            "Must supply exactly 1 non-null JAAS mechanism configuration (size was %d)",
                            jaasConfigEntries.size()
                    )
            );
        }
    }

    public boolean containsKey(String name) {
        return options.containsKey(name);
    }

    public boolean shouldCreateSSLSocketFactory(URL url) {
        return url.getProtocol().equalsIgnoreCase("https");
    }

    public Map<String, ?> getSslClientConfig() {
        ConfigDef sslConfigDef = new ConfigDef();
        sslConfigDef.withClientSslSupport();
        AbstractConfig sslClientConfig = new AbstractConfig(sslConfigDef, options);
        return sslClientConfig.values();
    }

    public SSLSocketFactory createSSLSocketFactory() {
        Map<String, ?> sslClientConfig = getSslClientConfig();
        SslFactory sslFactory = new SslFactory(ConnectionMode.CLIENT);
        sslFactory.configure(sslClientConfig);
        SSLSocketFactory socketFactory = ((DefaultSslEngineFactory) sslFactory.sslEngineFactory()).sslContext().getSocketFactory();
        log.debug("Created SSLSocketFactory: {}", sslClientConfig);
        return socketFactory;
    }

    public String validatePassword(String name) {
        Password value = (Password) options.get(name);

        if (value == null || Utils.isBlank(value.value()))
            throw new ConfigException(String.format("The OAuth configuration option %s value is required", name));

        return value.value().trim();
    }

    public String validateString(String name) {
        return validateString(name, true);
    }

    public String validateString(String name, boolean isRequired) {
        String value = (String) options.get(name);

        if (Utils.isBlank(value)) {
            if (isRequired)
                throw new ConfigException(String.format("The OAuth configuration option %s value is required", name));
            else
                return null;
        }

        return value.trim();
    }
}
