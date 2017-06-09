/*
 * Copyright 2015 MongoDB, Inc.
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

package com.mongodb.async.client

import com.mongodb.async.FutureResultCallback
import com.mongodb.connection.ClusterSettings
import com.mongodb.connection.ServerSettings
import com.mongodb.event.ClusterListener
import com.mongodb.event.CommandListener
import com.mongodb.event.ConnectionListener
import com.mongodb.event.ConnectionPoolListener
import com.mongodb.event.EventListenerSettings
import com.mongodb.event.ServerListener
import com.mongodb.event.ServerMonitorListener
import org.bson.Document

import static java.util.concurrent.TimeUnit.SECONDS

@SuppressWarnings('deprecation')
class MongoClientListenerRegistrationSpecification extends FunctionalSpecification {

    def 'should register event listeners'() {
        given:
        def clusterListener = Mock(ClusterListener) {
            (1.._) * _
        }
        def commandListener = Mock(CommandListener) {
            (1.._) * _
        }
        def connectionListener = Mock(ConnectionListener) {
            (1.._) * _
        }
        def connectionPoolListener = Mock(ConnectionPoolListener) {
            (1.._) * _
        }
        def serverListener = Mock(ServerListener) {
            (1.._) * _
        }
        def serverMonitorListener = Mock(ServerMonitorListener) {
            (1.._) * _
        }

        when:
        def eventListenerSettings = EventListenerSettings.builder()
                .addClusterListener(clusterListener)
                .addCommandListener(commandListener)
                .addConnectionListener(connectionListener)
                .addConnectionPoolListener(connectionPoolListener)
                .addServerListener(serverListener)
                .addServerMonitorListener(serverMonitorListener)
                .build()

        def clientSettings = Fixture.mongoClientBuilderFromConnectionString.eventListenerSettings(eventListenerSettings).build()
        def client = MongoClients.create(clientSettings)

        then:
        run(client.getDatabase('admin').&runCommand, new Document('ping', 1))
    }

    def 'should work with cluster and server setting event listeners'() {
        given:
        def clusterListener = Mock(ClusterListener) {
            (1.._) * _
        }
        def clusterListener2 = Mock(ClusterListener) {
            (1.._) * _
        }
        def commandListener = Mock(CommandListener) {
            (1.._) * _
        }
        def connectionListener = Mock(ConnectionListener) {
            (1.._) * _
        }
        def connectionPoolListener = Mock(ConnectionPoolListener) {
            (1.._) * _
        }
        def serverListener = Mock(ServerListener) {
            (1.._) * _
        }
        def serverListener2 = Mock(ServerListener) {
            (1.._) * _
        }
        def serverMonitorListener = Mock(ServerMonitorListener) {
            (1.._) * _
        }
        def serverMonitorListener2 = Mock(ServerMonitorListener) {
            (1.._) * _
        }

        when:
        def eventListenerSettings = EventListenerSettings.builder()
                .addClusterListener(clusterListener)
                .addCommandListener(commandListener)
                .addConnectionListener(connectionListener)
                .addConnectionPoolListener(connectionPoolListener)
                .addServerListener(serverListener)
                .addServerMonitorListener(serverMonitorListener)
                .build()
        def settings = Fixture.mongoClientBuilderFromConnectionString.eventListenerSettings(eventListenerSettings).build()
        def clusterSettings = ClusterSettings.builder(settings.getClusterSettings()).addClusterListener(clusterListener2).build()
        def serverSettings = ServerSettings.builder(settings.getServerSettings())
                .addServerListener(serverListener2)
                .addServerMonitorListener(serverMonitorListener2)
                .build()
        def clientSettings = MongoClientSettings.builder(settings)
                .clusterSettings(clusterSettings)
                .serverSettings(serverSettings)
                .build()
        def client = MongoClients.create(clientSettings)

        then:
        run(client.getDatabase('admin').&runCommand, new Document('ping', 1))
    }

    def 'should register single command listener'() {
        given:
        def listener = Mock(CommandListener)

        when:
        run(client(listener).getDatabase('admin').&runCommand, new Document('ping', 1))

        then:
        1 * listener.commandStarted(_)
        1 * listener.commandSucceeded(_)

        where:
        client << [
            { cmdListener ->
                MongoClients.create(Fixture.mongoClientBuilderFromConnectionString
                .eventListenerSettings(EventListenerSettings.builder().addCommandListener(cmdListener).build()).build()) },
            { cmdListener ->
                MongoClients.create(Fixture.mongoClientBuilderFromConnectionString.addCommandListener(cmdListener).build()) } ]
    }

    def 'should register multiple command listeners'() {
        given:
        def first = Mock(CommandListener)
        def second = Mock(CommandListener)

        when:
        run(client(first, second).getDatabase('admin').&runCommand, new Document('ping', 1))

        then:
        1 * first.commandStarted(_)
        1 * second.commandStarted(_)
        1 * first.commandSucceeded(_)
        1 * second.commandSucceeded(_)

        where:
        client << [
                { cmdListener1, cmdListener2  ->
                    MongoClients.create(Fixture.mongoClientBuilderFromConnectionString
                        .eventListenerSettings(EventListenerSettings.builder().addCommandListener(cmdListener1)
                        .addCommandListener(cmdListener2).build()).build()) },
                { cmdListener1, cmdListener2 ->
                    MongoClients.create(Fixture.mongoClientBuilderFromConnectionString
                        .addCommandListener(cmdListener1).addCommandListener(cmdListener2).build()) } ]
    }

    def run(operation, ... args) {
        def futureResultCallback = new FutureResultCallback()
        def opArgs = (args != null) ? args : []
        operation.call(*opArgs + futureResultCallback)
        futureResultCallback.get(60, SECONDS)
    }

}
