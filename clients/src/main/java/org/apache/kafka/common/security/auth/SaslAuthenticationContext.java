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
package org.apache.kafka.common.security.auth;

import java.net.InetAddress;
import java.util.Optional;

import javax.net.ssl.SSLSession;
import javax.security.sasl.SaslServer;

public class SaslAuthenticationContext implements AuthenticationContext {
    private final SaslServer server;
    private final SecurityProtocol securityProtocol;
    private final InetAddress clientAddress;
    private final String listenerName;
    private final Optional<SSLSession> sslSession;

    public SaslAuthenticationContext(SaslServer server, SecurityProtocol securityProtocol, InetAddress clientAddress, String listenerName) {
        this(server, securityProtocol, clientAddress, listenerName, Optional.empty());
    }

    public SaslAuthenticationContext(SaslServer server, SecurityProtocol securityProtocol,
                                     InetAddress clientAddress,
                                     String listenerName,
                                     Optional<SSLSession> sslSession) {
        this.server = server;
        this.securityProtocol = securityProtocol;
        this.clientAddress = clientAddress;
        this.listenerName = listenerName;
        this.sslSession = sslSession;
    }

    public SaslServer server() {
        return server;
    }

    /**
     * Returns SSL session for the connection if security protocol is SASL_SSL. If SSL
     * mutual client authentication is enabled for the listener, peer principal can be
     * determined using {@link SSLSession#getPeerPrincipal()}.
     */
    public Optional<SSLSession> sslSession() {
        return sslSession;
    }

    @Override
    public SecurityProtocol securityProtocol() {
        return securityProtocol;
    }

    @Override
    public InetAddress clientAddress() {
        return clientAddress;
    }

    @Override
    public String listenerName() {
        return listenerName;
    }
}
