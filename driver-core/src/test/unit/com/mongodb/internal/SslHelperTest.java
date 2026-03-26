/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.internal;

import com.mongodb.ServerAddress;
import com.mongodb.internal.connection.SslHelper;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLParameters;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SslHelperTest {

    @Test
    void shouldEnableHttpsHostNameVerification() {
        SSLParameters sslParameters = new SSLParameters();
        SslHelper.enableHostNameVerification(sslParameters);
        assertEquals("HTTPS", sslParameters.getEndpointIdentificationAlgorithm());
    }

    @Test
    void shouldEnableServerNameIndicator() {
        String serverName = "server.me";
        SSLParameters sslParameters = new SSLParameters();
        SslHelper.enableSni(new ServerAddress(serverName).getHost(), sslParameters);
        assertEquals(Collections.singletonList(new SNIHostName(serverName)), sslParameters.getServerNames());
    }
}
