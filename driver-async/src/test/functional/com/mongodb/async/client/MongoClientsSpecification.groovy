/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.mongodb.async.client

import com.mongodb.MongoCredential
import com.mongodb.ReadConcern
import com.mongodb.ServerAddress
import com.mongodb.Tag
import com.mongodb.TagSet
import com.mongodb.WriteConcern
import com.mongodb.connection.AsynchronousSocketChannelStreamFactoryFactory
import com.mongodb.connection.netty.NettyStreamFactoryFactory
import spock.lang.IgnoreIf
import spock.lang.Unroll

import static com.mongodb.ClusterFixture.getSslSettings
import static com.mongodb.ReadPreference.primary
import static com.mongodb.ReadPreference.secondaryPreferred
import static java.util.Arrays.asList
import static java.util.concurrent.TimeUnit.MILLISECONDS

class MongoClientsSpecification extends FunctionalSpecification {

    def 'should apply connection string to cluster settings'() {
        when:
        def client = MongoClients.create('mongodb://localhost,localhost:27018/')

        then:
        client.settings.clusterSettings.hosts == [new ServerAddress('localhost'), new ServerAddress('localhost:27018')]

        cleanup:
        client?.close()
    }

    def 'should apply connection string to credential list'() {
        when:
        def client = MongoClients.create('mongodb://u:p@localhost/')

        then:
        client.settings.credentialList == [MongoCredential.createCredential('u', 'admin', 'p'.toCharArray())]

        cleanup:
        client?.close()
    }

    def 'should apply connection string to server settings'() {
        when:
        def client = MongoClients.create('mongodb://localhost/?heartbeatFrequencyMS=50')

        then:
        client.settings.serverSettings.getHeartbeatFrequency(MILLISECONDS) == 50

        cleanup:
        client?.close()
    }

    def 'should apply connection string to connection pool settings'() {
        when:
        def client = MongoClients.create('mongodb://localhost/?maxIdleTimeMS=200&maxLifeTimeMS=300')

        then:
        client.settings.connectionPoolSettings.getMaxConnectionIdleTime(MILLISECONDS) == 200
        client.settings.connectionPoolSettings.getMaxConnectionLifeTime(MILLISECONDS) == 300

        cleanup:
        client?.close()
    }

    def 'should apply connection string to ssl settings'() {
        when:
        def client = MongoClients.create('mongodb://localhost/?ssl=true&sslInvalidHostNameAllowed=true&streamType=netty')

        then:
        client.settings.sslSettings.enabled
        client.settings.sslSettings.invalidHostNameAllowed

        cleanup:
        client?.close()
    }

    def 'should apply connection string to netty stream type'() {
        when:
        def client = MongoClients.create('mongodb://localhost/?streamType=Netty')

        then:
        client.settings.streamFactoryFactory instanceof NettyStreamFactoryFactory

        cleanup:
        client?.close()
    }

    @IgnoreIf({ javaVersion < 1.7 || getSslSettings().isEnabled() })
    def 'should apply connection string to nio2 stream type'() {
        when:
        def client = MongoClients.create('mongodb://localhost/?streamType=NIO2')

        then:
        client.settings.streamFactoryFactory instanceof AsynchronousSocketChannelStreamFactoryFactory

        cleanup:
        client?.close()
    }

    def 'should apply connection string to socket settings'() {
        when:
        def client = MongoClients.create('mongodb://localhost/?connectTimeoutMS=300')

        then:
        client.settings.socketSettings.getConnectTimeout(MILLISECONDS) == 300

        cleanup:
        client?.close()
    }

    @Unroll
    def 'should correct parse read preference for #readPreference'() {
        when:
        def client = MongoClients.create(uri)

        then:
        client.settings.getReadPreference() == readPreference

        cleanup:
        client?.close()

        where:
        uri                                                              | readPreference
        'mongodb://localhost/?readPreference=secondaryPreferred'         | secondaryPreferred()
        'mongodb://localhost/?slaveOk=true'                              | secondaryPreferred()
        'mongodb://localhost/?slaveOk=false'                             | primary()
        'mongodb://localhost/' +
                '?readPreference=secondaryPreferred' +
                '&readPreferenceTags=dc:ny,rack:1' +
                '&readPreferenceTags=dc:ny' +
                '&readPreferenceTags='                                   | secondaryPreferred([
                                                                            new TagSet(asList(new Tag('dc', 'ny'), new Tag('rack', '1'))),
                                                                            new TagSet(asList(new Tag('dc', 'ny'))),
                                                                            new TagSet()])
    }

    @Unroll
    def 'should correct parse read concern for #readConcern'() {
        when:
        def client = MongoClients.create(uri)

        then:
        client.settings.getReadConcern() == readConcern

        cleanup:
        client?.close()

        where:
        uri                                               | readConcern
        'mongodb://localhost/'                            | ReadConcern.DEFAULT
        'mongodb://localhost/?readConcernLevel=local'     | ReadConcern.LOCAL
        'mongodb://localhost/?readConcernLevel=majority'  | ReadConcern.MAJORITY
    }

    @Unroll
    def 'should correctly parse different #writeConcern'() {
        when:
        def client = MongoClients.create(uri)

        then:
        client.settings.getWriteConcern() == writeConcern

        cleanup:
        client?.close()

        where:
        uri                                                            | writeConcern
        'mongodb://localhost'                                          | WriteConcern.ACKNOWLEDGED
        'mongodb://localhost/?safe=true'                               | WriteConcern.ACKNOWLEDGED
        'mongodb://localhost/?safe=false'                              | WriteConcern.UNACKNOWLEDGED
        'mongodb://localhost/?wTimeout=5'                              | WriteConcern.ACKNOWLEDGED.withWTimeout(5, MILLISECONDS)
        'mongodb://localhost/?fsync=true'                              | WriteConcern.ACKNOWLEDGED.withFsync(true)
        'mongodb://localhost/?journal=true'                            | WriteConcern.ACKNOWLEDGED.withJournal(true)
        'mongodb://localhost/?w=2&wtimeout=5&fsync=true&journal=true'  | new WriteConcern(2, 5, true, true)
        'mongodb://localhost/?w=majority&wtimeout=5&fsync=true&j=true' | new WriteConcern('majority', 5, true, true)
    }
}
