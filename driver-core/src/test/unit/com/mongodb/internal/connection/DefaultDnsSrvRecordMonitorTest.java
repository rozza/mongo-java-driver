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

import com.mongodb.MongoConfigurationException;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ClusterType;
import com.mongodb.internal.dns.DnsResolver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DefaultDnsSrvRecordMonitorTest {

    @Test
    void shouldResolveInitialHosts() throws InterruptedException {
        String hostName = "test1.test.build.10gen.cc";
        String srvServiceName = "mongodb";
        String resolvedHostOne = "localhost.test.build.10gen.cc:27017";
        String resolvedHostTwo = "localhost.test.build.10gen.cc:27018";
        List<String> expectedResolvedHosts = Arrays.asList(resolvedHostOne, resolvedHostTwo);
        TestDnsSrvRecordInitializer dnsSrvRecordInitializer = new TestDnsSrvRecordInitializer(ClusterType.REPLICA_SET, 1);
        DnsResolver dnsResolver = mock(DnsResolver.class);
        when(dnsResolver.resolveHostFromSrvRecords(hostName, srvServiceName)).thenReturn(expectedResolvedHosts);
        DefaultDnsSrvRecordMonitor monitor = new DefaultDnsSrvRecordMonitor(hostName, srvServiceName, 1, 10000,
                dnsSrvRecordInitializer, new ClusterId(), dnsResolver);

        try {
            monitor.start();
            List<Set<ServerAddress>> hostsLists = dnsSrvRecordInitializer.waitForInitializedHosts();

            assertEquals(Collections.singletonList(
                    new HashSet<>(Arrays.asList(new ServerAddress(resolvedHostOne), new ServerAddress(resolvedHostTwo)))),
                    hostsLists);
        } finally {
            monitor.close();
        }
    }

    @Test
    void shouldDiscoverNewResolvedHosts() throws InterruptedException {
        String hostName = "test1.test.build.10gen.cc";
        String srvServiceName = "mongodb";
        String resolvedHostOne = "localhost.test.build.10gen.cc:27017";
        String resolvedHostTwo = "localhost.test.build.10gen.cc:27018";
        String resolvedHostThree = "localhost.test.build.10gen.cc:27019";
        List<String> expectedResolvedHostsOne = Arrays.asList(resolvedHostOne, resolvedHostTwo);
        List<String> expectedResolvedHostsTwo = Arrays.asList(resolvedHostTwo, resolvedHostThree);
        TestDnsSrvRecordInitializer dnsSrvRecordInitializer = new TestDnsSrvRecordInitializer(ClusterType.SHARDED, 2);
        DnsResolver dnsResolver = mock(DnsResolver.class);
        when(dnsResolver.resolveHostFromSrvRecords(hostName, srvServiceName))
                .thenReturn(expectedResolvedHostsOne)
                .thenReturn(expectedResolvedHostsTwo);
        DefaultDnsSrvRecordMonitor monitor = new DefaultDnsSrvRecordMonitor(hostName, srvServiceName, 1, 1,
                dnsSrvRecordInitializer, new ClusterId(), dnsResolver);

        try {
            monitor.start();
            List<Set<ServerAddress>> hostsLists = dnsSrvRecordInitializer.waitForInitializedHosts();

            assertEquals(Arrays.asList(
                    new HashSet<>(Arrays.asList(new ServerAddress(resolvedHostOne), new ServerAddress(resolvedHostTwo))),
                    new HashSet<>(Arrays.asList(new ServerAddress(resolvedHostTwo), new ServerAddress(resolvedHostThree)))),
                    hostsLists);
        } finally {
            monitor.close();
        }
    }

    static Stream<Exception> initializationExceptionProvider() {
        return Stream.of(new MongoConfigurationException("test"), new NullPointerException());
    }

    @ParameterizedTest
    @MethodSource("initializationExceptionProvider")
    void shouldInitializeListenerWithException(Exception initializationException) throws InterruptedException {
        String hostName = "test1.test.build.10gen.cc";
        String srvServiceName = "mongodb";
        TestDnsSrvRecordInitializer dnsSrvRecordInitializer = new TestDnsSrvRecordInitializer(ClusterType.UNKNOWN, 1);
        DnsResolver dnsResolver = mock(DnsResolver.class);
        when(dnsResolver.resolveHostFromSrvRecords(hostName, srvServiceName)).thenThrow(initializationException);
        DefaultDnsSrvRecordMonitor monitor = new DefaultDnsSrvRecordMonitor(hostName, srvServiceName, 1, 10000,
                dnsSrvRecordInitializer, new ClusterId(), dnsResolver);

        try {
            monitor.start();
            List<MongoException> initializationExceptionList = dnsSrvRecordInitializer.waitForInitializedException();
            if (!(initializationException instanceof MongoException)) {
                // Unwrap the cause for non-MongoException types
                List<Throwable> causes = new ArrayList<>();
                for (MongoException e : initializationExceptionList) {
                    causes.add(e.getCause());
                }
                assertEquals(Collections.singletonList(initializationException), causes);
            } else {
                assertEquals(Collections.singletonList(initializationException), initializationExceptionList);
            }
        } finally {
            monitor.close();
        }
    }

    @ParameterizedTest
    @MethodSource("initializationExceptionProvider")
    void shouldNotInitializeListenerWithExceptionAfterSuccessfulInitialization(Exception initializationException)
            throws InterruptedException {
        String hostName = "test1.test.build.10gen.cc";
        String srvServiceName = "mongodb";
        List<String> resolvedHostListOne = Collections.singletonList("localhost.test.build.10gen.cc:27017");
        List<String> resolvedHostListTwo = Collections.singletonList("localhost.test.build.10gen.cc:27018");
        TestDnsSrvRecordInitializer dnsSrvRecordInitializer = new TestDnsSrvRecordInitializer(ClusterType.SHARDED, 2);
        DnsResolver dnsResolver = mock(DnsResolver.class);
        when(dnsResolver.resolveHostFromSrvRecords(hostName, srvServiceName))
                .thenReturn(resolvedHostListOne)
                .thenThrow(initializationException)
                .thenReturn(resolvedHostListTwo);
        DefaultDnsSrvRecordMonitor monitor = new DefaultDnsSrvRecordMonitor(hostName, srvServiceName, 1, 1,
                dnsSrvRecordInitializer, new ClusterId(), dnsResolver);

        try {
            monitor.start();
            List<MongoException> initializedExceptionList = dnsSrvRecordInitializer.waitForInitializedException();

            assertTrue(initializedExceptionList.isEmpty());
        } finally {
            monitor.close();
        }
    }

    static class TestDnsSrvRecordInitializer implements DnsSrvRecordInitializer {
        private final ClusterType clusterType;
        private final List<Set<ServerAddress>> hostsList = new ArrayList<>();
        private final List<MongoException> initializationExceptionList = new ArrayList<>();
        private final CountDownLatch latch;

        TestDnsSrvRecordInitializer(final ClusterType clusterType, final int expectedInitializations) {
            this.clusterType = clusterType;
            this.latch = new CountDownLatch(expectedInitializations);
        }

        List<Set<ServerAddress>> waitForInitializedHosts() throws InterruptedException {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("Timeout waiting for latch");
            }
            return hostsList;
        }

        List<MongoException> waitForInitializedException() throws InterruptedException {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("Timeout waiting for latch");
            }
            return initializationExceptionList;
        }

        @Override
        public void initialize(final Collection<ServerAddress> hosts) {
            if (latch.getCount() > 0) {
                hostsList.add(new HashSet<>(hosts));
                latch.countDown();
            }
        }

        @Override
        public void initialize(final MongoException initializationException) {
            if (latch.getCount() > 0) {
                initializationExceptionList.add(initializationException);
                latch.countDown();
            }
        }

        @Override
        public ClusterType getClusterType() {
            return clusterType;
        }
    }
}
