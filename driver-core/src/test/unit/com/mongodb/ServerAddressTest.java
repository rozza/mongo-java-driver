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

package com.mongodb;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ServerAddressTest {

    @ParameterizedTest
    @MethodSource("constructorParseData")
    @DisplayName("constructors should parse hostname and port correctly")
    void constructorsShouldParseHostnameAndPort(final ServerAddress address, final String host, final int port) {
        assertEquals(host, address.getHost());
        assertEquals(port, address.getPort());
        assertEquals(new ServerAddress(host, port), address);
    }

    static Stream<Object[]> constructorParseData() {
        return Stream.of(
                new Object[]{new ServerAddress(), ServerAddress.defaultHost(), ServerAddress.defaultPort()},
                new Object[]{new ServerAddress("10.0.0.1:1000"), "10.0.0.1", 1000},
                new Object[]{new ServerAddress("10.0.0.1"), "10.0.0.1", ServerAddress.defaultPort()},
                new Object[]{new ServerAddress("10.0.0.1", 1000), "10.0.0.1", 1000},
                new Object[]{new ServerAddress("somewhere"), "somewhere", ServerAddress.defaultPort()},
                new Object[]{new ServerAddress("SOMEWHERE"), "somewhere", ServerAddress.defaultPort()},
                new Object[]{new ServerAddress("somewhere:1000"), "somewhere", 1000},
                new Object[]{new ServerAddress("somewhere", 1000), "somewhere", 1000},
                new Object[]{new ServerAddress("[2010:836B:4179::836B:4179]"), "2010:836b:4179::836b:4179",
                        ServerAddress.defaultPort()},
                new Object[]{new ServerAddress("[2010:836B:4179::836B:4179]:1000"), "2010:836b:4179::836b:4179", 1000},
                new Object[]{new ServerAddress("[2010:836B:4179::836B:4179]", 1000), "2010:836b:4179::836b:4179", 1000},
                new Object[]{new ServerAddress("2010:836B:4179::836B:4179"), "2010:836b:4179::836b:4179",
                        ServerAddress.defaultPort()},
                new Object[]{new ServerAddress("2010:836B:4179::836B:4179", 1000), "2010:836b:4179::836b:4179", 1000}
        );
    }

    @Test
    @DisplayName("ipv4 host with a port specified should throw when a port is also specified as an argument")
    void ipv4HostWithPortShouldThrow() {
        assertThrows(IllegalArgumentException.class, () -> new ServerAddress("10.0.0.1:80", 80));
        assertThrows(IllegalArgumentException.class, () -> new ServerAddress("10.0.0.1:1000", 80));
    }

    @Test
    @DisplayName("ipv6 host with a port specified should throw when a port is also specified as an argument")
    void ipv6HostWithPortShouldThrow() {
        assertThrows(IllegalArgumentException.class, () -> new ServerAddress("[2010:836B:4179::836B:4179]:80", 80));
        assertThrows(IllegalArgumentException.class, () -> new ServerAddress("[2010:836B:4179::836B:4179]:1000", 80));
    }

    @Test
    @DisplayName("ipv6 host should throw when terminating ] is not specified")
    void ipv6HostShouldThrowWhenTerminatingBracketMissing() {
        assertThrows(IllegalArgumentException.class, () -> new ServerAddress("[2010:836B:4179::836B:4179"));
    }

    @Test
    @DisplayName("hostname with a port specified should throw when a port is also specified as an argument")
    void hostnameWithPortShouldThrow() {
        assertThrows(IllegalArgumentException.class, () -> new ServerAddress("somewhere:80", 80));
        assertThrows(IllegalArgumentException.class, () -> new ServerAddress("somewhere:1000", 80));
    }

    @Test
    @DisplayName("uri missing port should throw an exception")
    void uriMissingPortShouldThrow() {
        assertThrows(MongoException.class, () -> new ServerAddress("mongodb://somewhere/"));
    }
}
