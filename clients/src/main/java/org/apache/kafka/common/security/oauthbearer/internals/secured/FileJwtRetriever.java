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

import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.nio.file.Path;

/**
 * <code>FileJwtRetriever</code> is an {@link JwtRetriever} that will load the contents
 * of a file, interpreting them as a JWT access key in the serialized form.
 *
 * @see JwtRetriever
 */

public class FileJwtRetriever implements JwtRetriever {

    private final Path accessTokenFile;

    private String accessToken;

    public FileJwtRetriever(Path accessTokenFile) {
        this.accessTokenFile = accessTokenFile;
    }

    @Override
    public void init() throws IOException {
        this.accessToken = Utils.readFileAsString(accessTokenFile.toFile().getPath());
        // always non-null; to remove any newline chars or backend will report err
        this.accessToken = this.accessToken.trim();
    }

    @Override
    public String retrieve() throws IOException {
        if (accessToken == null)
            throw new IllegalStateException("Access token is null; please call init() first");

        return accessToken;
    }

}
