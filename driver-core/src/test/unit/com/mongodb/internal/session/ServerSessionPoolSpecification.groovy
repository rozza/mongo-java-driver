/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.internal.session

import com.mongodb.MongoException
import com.mongodb.ServerAddress
import com.mongodb.session.ServerSession
import com.mongodb.connection.Cluster
import com.mongodb.connection.ClusterDescription
import com.mongodb.connection.ClusterSettings
import com.mongodb.connection.Connection
import com.mongodb.connection.Server
import com.mongodb.connection.ServerDescription
import com.mongodb.connection.ServerSettings
import com.mongodb.internal.connection.NoOpSessionContext
import com.mongodb.internal.validator.NoOpFieldNameValidator
import com.mongodb.selector.ReadPreferenceServerSelector
import org.bson.BsonArray
import org.bson.BsonBinarySubType
import org.bson.BsonDocument
import org.bson.codecs.BsonDocumentCodec
import spock.lang.Specification

import java.util.concurrent.CountDownLatch

import static com.mongodb.ReadPreference.primaryPreferred
import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE
import static com.mongodb.connection.ClusterType.REPLICA_SET
import static com.mongodb.connection.ServerConnectionState.CONNECTED
import static com.mongodb.connection.ServerConnectionState.CONNECTING
import static com.mongodb.connection.ServerType.REPLICA_SET_PRIMARY
import static com.mongodb.connection.ServerType.UNKNOWN
import static java.util.concurrent.TimeUnit.MINUTES

class ServerSessionPoolSpecification extends Specification {

    def connectedDescription = new ClusterDescription(MULTIPLE, REPLICA_SET,
            [
                    ServerDescription.builder().ok(true)
                            .state(CONNECTED)
                            .address(new ServerAddress())
                            .type(REPLICA_SET_PRIMARY)
                            .logicalSessionTimeoutMinutes(30)
                            .build()
            ], ClusterSettings.builder().hosts([new ServerAddress()]).build(), ServerSettings.builder().build())

    def unconnectedDescription = new ClusterDescription(MULTIPLE, REPLICA_SET,
            [
                    ServerDescription.builder().ok(true)
                            .state(CONNECTING)
                            .address(new ServerAddress())
                            .type(UNKNOWN)
                            .logicalSessionTimeoutMinutes(null)
                            .build()
            ], ClusterSettings.builder().hosts([new ServerAddress()]).build(), ServerSettings.builder().build())

    def 'should get session'() {
        given:
        def cluster = Stub(Cluster) {
            getDescription() >> connectedDescription
        }
        def pool = new ServerSessionPool(cluster)

        when:
        def session = getSession(async, pool)

        then:
        session != null

        where:
        async << [true, false]
    }

    def 'should throw IllegalStateException if pool is closed'() {
        given:
        def cluster = Stub(Cluster) {
            getDescription() >> connectedDescription
        }
        def pool = new ServerSessionPool(cluster)
        pool.close()

        when:
        getSession(async, pool)

        then:
        thrown(IllegalStateException)

        where:
        async << [true, false]
    }

    def 'should pool session'() {
        given:
        def cluster = Stub(Cluster) {
            getDescription() >> connectedDescription
        }
        def pool = new ServerSessionPool(cluster)
        def session = getSession(async, pool)

        when:
        pool.release(session)
        def pooledSession = getSession(async, pool)

        then:
        session == pooledSession

        where:
        async << [true, false]
    }

    def 'should prune sessions on release'() {
        given:
        def cluster = Mock(Cluster) {
            getDescription() >> connectedDescription
        }
        def clock = Stub(ServerSessionPool.Clock) {
            millis() >>> [0, 0,                          // first get
                          1, 1,                          // second get
                          2, 2,                          // third get
                          3,                             // first release
                          MINUTES.toMillis(29),       // second release
                          MINUTES.toMillis(29) + 2,   // third release
                          MINUTES.toMillis(29) + 2,
                          MINUTES.toMillis(29) + 2
            ]
        }
        def pool = new ServerSessionPool(cluster, clock)
        def sessionOne = getSession(async, pool)
        def sessionTwo = getSession(async, pool)
        def sessionThree = getSession(async, pool)

        when:
        pool.release(sessionOne)

        then:
        !sessionOne.closed

        when:
        pool.release(sessionTwo)

        then:
        !sessionOne.closed
        !sessionTwo.closed

        when:
        pool.release(sessionThree)

        then:
        sessionOne.closed
        sessionTwo.closed
        !sessionThree.closed
        0 * cluster.selectServer(_)

        where:
        async << [true, false]
    }

    def 'should prune sessions when getting'() {
        given:
        def cluster = Mock(Cluster) {
            getDescription() >> connectedDescription
        }
        def clock = Stub(ServerSessionPool.Clock) {
            millis() >>> [0, 0,                          // first get
                          0,                             // first release
                          MINUTES.toMillis(29) + 1,   // second get
            ]
        }
        def pool = new ServerSessionPool(cluster, clock)
        def sessionOne = getSession(async, pool)

        when:
        pool.release(sessionOne)

        then:
        !sessionOne.closed

        when:
        def sessionTwo = getSession(async, pool)

        then:
        sessionTwo != sessionOne
        sessionOne.closed
        0 * cluster.selectServer(_)

        where:
        async << [true, false]
    }

    def 'should not prune session when timeout is null'() {
        given:
        def cluster = Stub(Cluster) {
            getDescription() >> unconnectedDescription
        }
        def clock = Stub(ServerSessionPool.Clock) {
            millis() >>> [0, 0,
                          MINUTES.toMillis(29) + 1]
        }
        def pool = new ServerSessionPool(cluster, clock)
        def session = getSession(async, pool)

        when:
        pool.release(session)
        def newSession = getSession(async, pool)

        then:
        session == newSession

        where:
        async << [true, false]
    }

    def 'should initialize session'() {
        given:
        def cluster = Stub(Cluster) {
            getDescription() >> connectedDescription
        }
        def clock = Stub(ServerSessionPool.Clock) {
            millis() >> 42
        }
        def pool = new ServerSessionPool(cluster, clock)

        when:
        def session = getSession(async, pool) as ServerSessionPool.ServerSessionImpl

        then:
        session.lastUsedAtMillis == 42
        session.transactionNumber == 0
        def uuid = session.identifier.getBinary('id')
        uuid != null
        uuid.type == BsonBinarySubType.UUID_STANDARD.value
        uuid.data.length == 16
        session.advanceTransactionNumber() == 0
        session.advanceTransactionNumber() == 1

        where:
        async << [true, false]
    }

    def 'should end pooled sessions when pool is closed'() {
        given:
        def connection = Mock(Connection)
        def server = Stub(Server) {
            getConnection() >> connection
        }
        def cluster = Mock(Cluster) {
            getDescription() >> connectedDescription
        }
        def pool = new ServerSessionPool(cluster)
        // check out sessions up the the endSessions batch size
        def sessions = []
        10000.times { sessions.add(getSession(async, pool)) }
        // and then check out one more
        def oneOverBatchSizeSession = getSession(async, pool)

        // now release them all before closing the pool
        for (def cur : sessions) {
            pool.release(cur)
        }
        pool.release(oneOverBatchSizeSession)

        when:
        pool.close()

        then:
        // first batch is the first 10K sessions, final batch is the last one
        1 * cluster.selectServer { (it as ReadPreferenceServerSelector).readPreference == primaryPreferred() }  >> server
        1 * connection.command('admin',
                new BsonDocument('endSessions', new BsonArray(sessions*.getIdentifier())),
                { it instanceof NoOpFieldNameValidator }, primaryPreferred(),
                { it instanceof BsonDocumentCodec }, NoOpSessionContext.INSTANCE) >> new BsonDocument()
        1 * connection.release()

        1 * cluster.selectServer { (it as ReadPreferenceServerSelector).readPreference == primaryPreferred() }  >> server
        1 * connection.command('admin',
                new BsonDocument('endSessions', new BsonArray([oneOverBatchSizeSession.getIdentifier()])),
                { it instanceof NoOpFieldNameValidator }, primaryPreferred(),
                { it instanceof BsonDocumentCodec }, NoOpSessionContext.INSTANCE) >> new BsonDocument()
        1 * connection.release()

        where:
        async << [true, false]
    }

    def getSession(boolean async, ServerSessionPool pool) {
        if (async) {
            getServerSessionAsync(pool)
        } else {
            pool.get()
        }
    }

    def getServerSessionAsync(ServerSessionPool pool) {
        def serverLatch = new ServerSessionLatch()
        pool.getAsync { ServerSession result, MongoException e ->
            serverLatch.serverSession = result
            serverLatch.throwable = e
            serverLatch.latch.countDown()
        }
        serverLatch.get()
    }

    class ServerSessionLatch {
        CountDownLatch latch = new CountDownLatch(1)
        ServerSession serverSession
        Throwable throwable

        def get() {
            latch.await()
            if (throwable != null) {
                throw throwable
            }
            serverSession
        }
    }
}
