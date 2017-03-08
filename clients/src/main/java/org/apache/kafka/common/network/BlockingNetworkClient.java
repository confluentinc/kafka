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

package org.apache.kafka.common.network;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.utils.Time;

import java.io.IOException;
import java.util.List;

public class BlockingNetworkClient {
    private final KafkaClient client;

    public BlockingNetworkClient(KafkaClient client) {
        this.client = client;
    }

    /**
     * Checks whether the node is currently connected, first calling `client.poll` to ensure that any pending
     * disconnects have been processed.
     *
     * This method can be used to check the status of a connection prior to calling the blocking version to be able
     * to tell whether the latter completed a new connection.
     */
    private boolean isReady(Node node, long currentTime) {
       client.poll(0, currentTime);
       return client.isReady(node, currentTime);
    }

    /**
     * Invokes `client.poll` to discard pending disconnects, followed by `client.ready` and 0 or more `client.poll`
     * invocations until the connection to `node` is ready, the timeout expires or the connection fails.
     *
     * It returns `true` if the call completes normally or `false` if the timeout expires. If the connection fails,
     * an `IOException` is thrown instead. Note that if the `NetworkClient` has been configured with a positive
     * connection timeout, it is possible for this method to raise an `IOException` for a previous connection which
     * has recently disconnected.
     *
     * This method is useful for implementing blocking behaviour on top of the non-blocking `NetworkClient`, use it with
     * care.
     */
    public boolean isReady(Node node, Time time, long timeout) throws IOException {
        if (timeout < 0) {
            throw new IllegalArgumentException("Timeout needs to be greater than 0");
        }
        long startTime = time.milliseconds();
        long expiryTime = startTime + timeout;

        long attemptStartTime = startTime;

        if (!isReady(node, time.milliseconds()))
            client.ready(node, time.milliseconds());

        while (!client.isReady(node, time.milliseconds()) && attemptStartTime < expiryTime) {
            if (client.connectionFailed(node)) {
                throw new IOException("Connection to " + node + " failed.");
            }
            long pollTimeout = expiryTime - attemptStartTime;
            client.poll(pollTimeout, time.milliseconds());
            attemptStartTime = time.milliseconds();
        }
        return client.isReady(node, time.milliseconds());
    }

    /**
     * Invokes `client.send` followed by 1 or more `client.poll` invocations until a response is received or a
     * disconnection happens (which can happen for a number of reasons including a request timeout).
     *
     * In case of a disconnection, an `IOException` is thrown.
     *
     * This method is useful for implementing blocking behaviour on top of the non-blocking `NetworkClient`, use it with
     * care.
     */
    public ClientResponse sendAndReceive(ClientRequest request, Time time) throws IOException {
        client.send(request, time.milliseconds());
        while (true) {
            List<ClientResponse> responses = client.poll(Long.MAX_VALUE, time.milliseconds());
            for (ClientResponse response : responses) {
                if (response.requestHeader().correlationId() == request.correlationId()) {
                    if (response.wasDisconnected()) {
                        throw new IOException("Connection to {} " + response.destination() + " was disconnected before ther response was read");
                    }
                    if (response.versionMismatch() != null) {
                        throw response.versionMismatch();
                    }
                    return response;
                }
            }
        }
    }
}
