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

package com.mongodb.internal.connection;

import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerId;
import com.mongodb.internal.inject.SameObjectProvider;
import com.mongodb.management.JMXConnectionPoolListener;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.mongodb.management.ConnectionPoolStatisticsMBean;

import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static com.mongodb.ClusterFixture.OPERATION_CONTEXT_FACTORY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class JMXConnectionPoolListenerTest {

    private static final ServerId SERVER_ID = new ServerId(new ClusterId(), new ServerAddress("host1", 27018));
    private final TestInternalConnectionFactory connectionFactory = new TestInternalConnectionFactory();
    private final JMXConnectionPoolListener jmxListener = new JMXConnectionPoolListener();

    @Test
    void statisticsShouldReflectValuesFromTheProvider() {
        DefaultConnectionPool provider = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                ConnectionPoolSettings.builder().minSize(0).maxSize(5)
                        .addConnectionPoolListener(jmxListener).build(), mockSdamProvider(), OPERATION_CONTEXT_FACTORY);
        provider.ready();

        try {
            provider.get(OPERATION_CONTEXT);
            provider.get(OPERATION_CONTEXT).close();

            assertNotNull(getMBean(jmxListener,SERVER_ID));
            assertEquals(SERVER_ID.getAddress().getHost(), getMBean(jmxListener,SERVER_ID).getHost());
            assertEquals(SERVER_ID.getAddress().getPort(), getMBean(jmxListener,SERVER_ID).getPort());
            assertEquals(0, getMBean(jmxListener,SERVER_ID).getMinSize());
            assertEquals(5, getMBean(jmxListener,SERVER_ID).getMaxSize());
            assertEquals(2, getMBean(jmxListener,SERVER_ID).getSize());
            assertEquals(1, getMBean(jmxListener,SERVER_ID).getCheckedOutCount());
        } finally {
            provider.close();
        }
    }

    @Test
    void shouldAddMBean() throws Exception {
        DefaultConnectionPool provider = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                ConnectionPoolSettings.builder().minSize(0).maxSize(5)
                        .addConnectionPoolListener(jmxListener).build(), mockSdamProvider(), OPERATION_CONTEXT_FACTORY);

        try {
            assertTrue(ManagementFactory.getPlatformMBeanServer().isRegistered(
                    new ObjectName(getMBeanObjectName(jmxListener,SERVER_ID))));
        } finally {
            provider.close();
        }
    }

    @Test
    void shouldRemoveMBean() throws Exception {
        DefaultConnectionPool provider = new DefaultConnectionPool(SERVER_ID, connectionFactory,
                ConnectionPoolSettings.builder().minSize(0).maxSize(5)
                        .addConnectionPoolListener(jmxListener).build(), mockSdamProvider(), OPERATION_CONTEXT_FACTORY);

        provider.close();

        assertNull(getMBean(jmxListener,SERVER_ID));
        assertFalse(ManagementFactory.getPlatformMBeanServer().isRegistered(
                new ObjectName(getMBeanObjectName(jmxListener,SERVER_ID))));
    }

    @Test
    void shouldCreateValidObjectNameForHostname() throws Exception {
        String beanName = getMBeanObjectName(jmxListener,SERVER_ID);
        ObjectName objectName = new ObjectName(beanName);

        assertEquals("org.mongodb.driver:type=ConnectionPool,clusterId=" + SERVER_ID.getClusterId().getValue()
                + ",host=" + SERVER_ID.getAddress().getHost()
                + ",port=" + SERVER_ID.getAddress().getPort(), objectName.toString());
    }

    @Test
    void shouldCreateValidObjectNameForIpv4Addresses() throws Exception {
        ServerId serverId = new ServerId(new ClusterId(), new ServerAddress("127.0.0.1"));
        String beanName = getMBeanObjectName(jmxListener,serverId);
        ObjectName objectName = new ObjectName(beanName);

        assertEquals("org.mongodb.driver:type=ConnectionPool,clusterId=" + serverId.getClusterId().getValue()
                + ",host=127.0.0.1,port=27017", objectName.toString());
    }

    @Test
    void shouldCreateValidObjectNameForIpv6Address() throws Exception {
        ServerId serverId = new ServerId(new ClusterId(), new ServerAddress("[::1]"));
        String beanName = getMBeanObjectName(jmxListener,serverId);
        ObjectName objectName = new ObjectName(beanName);

        assertEquals("org.mongodb.driver:type=ConnectionPool,clusterId=" + serverId.getClusterId().getValue()
                + ",host=\"::1\",port=27017", objectName.toString());
    }

    @Test
    void shouldIncludeDescriptionInObjectNameIfSet() {
        ServerId serverId = new ServerId(new ClusterId(), new ServerAddress());
        assertFalse(getMBeanObjectName(jmxListener,serverId).contains("description"));

        serverId = new ServerId(new ClusterId("my app server"), new ServerAddress());
        assertTrue(getMBeanObjectName(jmxListener,serverId).contains("description"));
    }

    @ParameterizedTest
    @CsvSource({
            "cluster Id,  cluster Id,  host name,  host name,  client description,  client description",
            "'cluster,Id', '\"cluster,Id\"', 'host,name', '\"host,name\"', 'client, description', '\"client, description\"'",
            "'cluster:Id', '\"cluster:Id\"', hostname,   hostname,   'client: description', '\"client: description\"'",
            "'cluster=Id', '\"cluster=Id\"', 'host=name', '\"host=name\"', 'client= description', '\"client= description\"'",
    })
    void shouldQuoteValuesContainingSpecialCharacters(String clusterIdName, String expectedClusterIdName,
                                                       String host, String expectedHost,
                                                       String description, String expectedDescription) throws Exception {
        ClusterId clusterId = createClusterId(clusterIdName, description);
        ServerId serverId = new ServerId(clusterId, new ServerAddress(host));
        ObjectName objectName = new ObjectName(getMBeanObjectName(jmxListener,serverId));

        assertEquals("org.mongodb.driver:type=ConnectionPool,clusterId=" + expectedClusterIdName
                + ",host=" + expectedHost + ",port=27017,description=" + expectedDescription, objectName.toString());
    }

    private SameObjectProvider<SdamServerDescriptionManager> mockSdamProvider() {
        return SameObjectProvider.initialized(mock(SdamServerDescriptionManager.class));
    }

    private static ClusterId createClusterId(String value, String description) {
        try {
            java.lang.reflect.Constructor<ClusterId> ctor = ClusterId.class.getDeclaredConstructor(String.class, String.class);
            ctor.setAccessible(true);
            return ctor.newInstance(value, description);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static ConnectionPoolStatisticsMBean getMBean(JMXConnectionPoolListener listener, ServerId serverId) {
        try {
            Method method = JMXConnectionPoolListener.class.getDeclaredMethod("getMBean", ServerId.class);
            method.setAccessible(true);
            return (ConnectionPoolStatisticsMBean) method.invoke(listener, serverId);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String getMBeanObjectName(JMXConnectionPoolListener listener, ServerId serverId) {
        try {
            Method method = JMXConnectionPoolListener.class.getDeclaredMethod("getMBeanObjectName", ServerId.class);
            method.setAccessible(true);
            return (String) method.invoke(listener, serverId);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
