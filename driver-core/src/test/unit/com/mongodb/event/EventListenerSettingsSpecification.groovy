/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.event

import spock.lang.Specification

class EventListenerSettingsSpecification extends Specification {
    def 'should have correct defaults'() {
        when:
        def settings = EventListenerSettings.builder().build()

        then:
        settings.getClusterListeners().isEmpty()
        settings.getConnectionListeners().isEmpty()
        settings.getConnectionPoolListeners().isEmpty()
        settings.getCommandListeners().isEmpty()
        settings.getServerListeners().isEmpty()
        settings.getServerMonitorListeners().isEmpty()
    }

    def 'should apply builder settings'() {
        when:
        def clusterListenerSettings1 = Stub(ClusterListener)
        def clusterListenerSettings2 = Stub(ClusterListener)
        def connectionListenerSettings1 = Stub(ConnectionListener)
        def connectionListenerSettings2 = Stub(ConnectionListener)
        def connectionPoolListenerSettings1 = Stub(ConnectionPoolListener)
        def connectionPoolListenerSettings2 = Stub(ConnectionPoolListener)
        def serverListenerSettings1 = Stub(ServerListener)
        def serverListenerSettings2 = Stub(ServerListener)
        def serverMonitorListenerSettings1 = Stub(ServerMonitorListener)
        def serverMonitorListenerSettings2 = Stub(ServerMonitorListener)

        def settings = EventListenerSettings.builder()
                .addClusterListener(clusterListenerSettings1)
                .addClusterListener(clusterListenerSettings2)
                .addConnectionListener(connectionListenerSettings1)
                .addConnectionListener(connectionListenerSettings2)
                .addConnectionPoolListener(connectionPoolListenerSettings1)
                .addConnectionPoolListener(connectionPoolListenerSettings2)
                .addServerListener(serverListenerSettings1)
                .addServerListener(serverListenerSettings2)
                .addServerMonitorListener(serverMonitorListenerSettings1)
                .addServerMonitorListener(serverMonitorListenerSettings2)
                .build()


        then:
        settings.getClusterListeners() == [clusterListenerSettings1, clusterListenerSettings2]
        settings.getConnectionListeners() == [connectionListenerSettings1, connectionListenerSettings2]
        settings.getConnectionPoolListeners() == [connectionPoolListenerSettings1, connectionPoolListenerSettings2]
        settings.getServerListeners() == [serverListenerSettings1, serverListenerSettings2]
        settings.getServerMonitorListeners() == [serverMonitorListenerSettings1, serverMonitorListenerSettings2]
    }
}
