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

package org.apache.kafka.tools;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenRetriever;
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenRetrieverFactory;
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenValidator;
import org.apache.kafka.common.security.oauthbearer.internals.secured.AccessTokenValidatorFactory;
import org.apache.kafka.common.security.oauthbearer.internals.secured.CloseableVerificationKeyResolver;
import org.apache.kafka.common.security.oauthbearer.internals.secured.JaasOptionsUtils;
import org.apache.kafka.common.security.oauthbearer.internals.secured.VerificationKeyResolverFactory;
import org.apache.kafka.common.utils.Exit;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;

public class OAuthCompatibilityTool {

    public static void main(String[] args) {
        String description = String.format(
            "This tool is used to verify OAuth/OIDC provider compatibility.%n%n" +
                "Run the following script to determine the configuration options:%n%n" +
                "    ./bin/kafka-run-class.sh %s --help",
            OAuthCompatibilityTool.class.getName());

        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("oauth-compatibility-tool")
            .defaultHelp(true)
            .description(description);
        parser.addArgument("-b", "--broker-configuration")
            .type(String.class)
            .metavar("broker-configuration")
            .dest("brokerConfigFileName")
            .required(true)
            .help("Local file name that contains the broker configuration");
        parser.addArgument("-c", "--client-configuration")
            .type(String.class)
            .metavar("client-configuration")
            .dest("clientConfigFileName")
            .required(true)
            .help("Local file name that contains the client configuration");

        Namespace namespace;

        try {
            namespace = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            Exit.exit(1);
            return;
        }

        Time time = Time.SYSTEM;

        try {
            String accessToken;

            {
                // Client side...
                Map<String, ?> configs = getConfigs(namespace.getString("clientConfigFileName"));
                JaasContext context = JaasContext.loadClientContext(configs);
                Map<String, Object> jaasConfigs = JaasOptionsUtils.getOptions(OAUTHBEARER_MECHANISM, context.configurationEntries());

                try (AccessTokenRetriever atr = AccessTokenRetrieverFactory.create(time, configs, jaasConfigs)) {
                    atr.init();
                    AccessTokenValidator atv = AccessTokenValidatorFactory.create(configs);
                    System.out.println("PASSED 1/5: client configuration");

                    accessToken = atr.retrieve();
                    System.out.println("PASSED 2/5: client JWT retrieval");

                    atv.validate(accessToken);
                    System.out.println("PASSED 3/5: client JWT validation");
                }
            }

            {
                // Broker side...
                Map<String, ?> configs = getConfigs(namespace.getString("brokerConfigFileName"));
                JaasContext context = JaasContext.loadClientContext(configs);
                Map<String, Object> jaasConfigs = JaasOptionsUtils.getOptions(OAUTHBEARER_MECHANISM, context.configurationEntries());

                try (CloseableVerificationKeyResolver vkr = VerificationKeyResolverFactory.create(configs, jaasConfigs)) {
                    vkr.init();
                    AccessTokenValidator atv = AccessTokenValidatorFactory.create(configs, vkr);
                    System.out.println("PASSED 4/5: broker configuration");

                    atv.validate(accessToken);
                    System.out.println("PASSED 5/5: broker JWT validation");
                }
            }

            System.out.println("SUCCESS");
            Exit.exit(0);
        } catch (Throwable t) {
            System.out.println("FAILED:");
            t.printStackTrace(System.err);

            if (t instanceof ConfigException) {
                System.out.printf("%n");
            }

            Exit.exit(1);
        }
    }

    private static Map<String, ?> getConfigs(String fileName) {
        try {
            Map<String, Object> config = new HashMap<>(Utils.propsToMap(Utils.loadProps(fileName)));

            // This here is going to fill in all the defaults for the values we don't specify...
            ConfigDef cd = new ConfigDef();
            SaslConfigs.addClientSaslSupport(cd);
            SslConfigs.addClientSslSupport(cd);
            return new AbstractConfig(cd, config).values();
        } catch (Exception e) {
            throw new KafkaException("Could not load configuration from " + fileName);
        }
    }
}
