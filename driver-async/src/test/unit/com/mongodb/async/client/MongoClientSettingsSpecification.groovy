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

package com.mongodb.async.client

import com.mongodb.MongoCompressor
import com.mongodb.MongoCredential
import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import com.mongodb.ServerAddress
import com.mongodb.WriteConcern
import com.mongodb.connection.ClusterSettings
import com.mongodb.connection.ConnectionPoolSettings
import com.mongodb.connection.ServerSettings
import com.mongodb.connection.SocketSettings
import com.mongodb.connection.SslSettings
import com.mongodb.connection.netty.NettyStreamFactoryFactory
import com.mongodb.event.CommandListener
import org.bson.codecs.configuration.CodecRegistry
import spock.lang.Specification

import static com.mongodb.CustomMatchers.isTheSameAs
import static spock.util.matcher.HamcrestSupport.expect

class MongoClientSettingsSpecification extends Specification {

    def 'should set the correct default values'() {
        given:
        def settings = MongoClientSettings.builder().build()

        expect:
        settings.getWriteConcern() == WriteConcern.ACKNOWLEDGED
        !settings.getRetryWrites()
        settings.getReadConcern() == ReadConcern.DEFAULT
        settings.getReadPreference() == ReadPreference.primary()
        settings.getCommandListeners().isEmpty()
        settings.getApplicationName() == null
        settings.getConnectionPoolSettings() == ConnectionPoolSettings.builder().build()
        settings.getSocketSettings() == SocketSettings.builder().build()
        settings.getHeartbeatSocketSettings() == SocketSettings.builder().build()
        settings.getServerSettings() == ServerSettings.builder().build()
        settings.getStreamFactoryFactory() == null
        settings.getCompressorList() == []
        settings.getCredentialList() == []
        settings.getCredential() == null
    }

    @SuppressWarnings('UnnecessaryObjectReferences')
    def 'should handle illegal arguments'() {
        given:
        def builder = MongoClientSettings.builder()

        when:
        builder.clusterSettings(null)
        then:
        thrown(IllegalArgumentException)

        when:
        builder.socketSettings(null)
        then:
        thrown(IllegalArgumentException)

        when:
        builder.heartbeatSocketSettings(null)
        then:
        thrown(IllegalArgumentException)

        when:
        builder.connectionPoolSettings(null)
        then:
        thrown(IllegalArgumentException)

        when:
        builder.serverSettings(null)
        then:
        thrown(IllegalArgumentException)

        when:
        builder.sslSettings(null)
        then:
        thrown(IllegalArgumentException)

        when:
        builder.readPreference(null)
        then:
        thrown(IllegalArgumentException)

        when:
        builder.writeConcern(null)
        then:
        thrown(IllegalArgumentException)

        when:
        builder.credential(null)
        then:
        thrown(IllegalArgumentException)

        when:
        builder.credentialList(null)
        then:
        thrown(IllegalArgumentException)

        when:
        builder.codecRegistry(null)
        then:
        thrown(IllegalArgumentException)

        when:
        builder.streamFactoryFactory(null)
        then:
        thrown(IllegalArgumentException)

        when:
        builder.addCommandListener(null)
        then:
        thrown(IllegalArgumentException)

        when:
        builder.compressorList(null)
        then:
        thrown(IllegalArgumentException)
    }

    def 'should build with supplied settings'() {
        given:
        def streamFactoryFactory = NettyStreamFactoryFactory.builder().build()
        def sslSettings = Stub(SslSettings)
        def socketSettings = Stub(SocketSettings)
        def serverSettings = Stub(ServerSettings)
        def heartbeatSocketSettings = Stub(SocketSettings)
        def credentialList = [MongoCredential.createMongoX509Credential('test')]
        def connectionPoolSettings = Stub(ConnectionPoolSettings)
        def codecRegistry = Stub(CodecRegistry)
        def commandListener = Stub(CommandListener)
        def clusterSettings = ClusterSettings.builder().hosts([new ServerAddress('localhost')]).requiredReplicaSetName('test').build()

        when:
        def settings = MongoClientSettings.builder()
                .readPreference(ReadPreference.secondary())
                .writeConcern(WriteConcern.JOURNALED)
                .retryWrites(true)
                .readConcern(ReadConcern.LOCAL)
                .applicationName('app1')
                .addCommandListener(commandListener)
                .sslSettings(sslSettings)
                .socketSettings(socketSettings)
                .serverSettings(serverSettings)
                .heartbeatSocketSettings(heartbeatSocketSettings)
                .credentialList(credentialList)
                .connectionPoolSettings(connectionPoolSettings)
                .codecRegistry(codecRegistry)
                .clusterSettings(clusterSettings)
                                         .streamFactoryFactory(streamFactoryFactory)
                .compressorList([MongoCompressor.createZlibCompressor()])
                .build()

        then:
        settings.getReadPreference() == ReadPreference.secondary()
        settings.getWriteConcern() == WriteConcern.JOURNALED
        settings.getRetryWrites()
        settings.getReadConcern() == ReadConcern.LOCAL
        settings.getApplicationName() == 'app1'
        settings.getCommandListeners().get(0) == commandListener
        settings.getConnectionPoolSettings() == connectionPoolSettings
        settings.getSocketSettings() == socketSettings
        settings.getHeartbeatSocketSettings() == heartbeatSocketSettings
        settings.getServerSettings() == serverSettings
        settings.getCodecRegistry() == codecRegistry
        settings.getCredentialList() == credentialList
        settings.getCredential() == credentialList.get(0)
        settings.getConnectionPoolSettings() == connectionPoolSettings
        settings.getClusterSettings() == clusterSettings
        settings.getStreamFactoryFactory() == streamFactoryFactory
        settings.getCompressorList() == [MongoCompressor.createZlibCompressor()]
    }

    def 'should support deprecated multiple credentials'() {
        given:
        def credentialList = [MongoCredential.createMongoX509Credential('test'), MongoCredential.createGSSAPICredential('gssapi')]

        when:
        def settings = MongoClientSettings.builder().credentialList(credentialList).build()

        then:
        settings.getCredentialList() == credentialList

        when:
        settings.getCredential()

        then:
        thrown(IllegalStateException)

        when:
        settings = MongoClientSettings.builder().credential(credentialList.get(0)).build()

        then:
        settings.getCredentialList() == [credentialList.get(0)]
        settings.getCredential() == credentialList.get(0)
    }

    def 'should be easy to create new settings from existing'() {
        when:
        def settings = MongoClientSettings.builder().build()

        then:
        expect settings, isTheSameAs(MongoClientSettings.builder(settings).build())

        when:
        def sslSettings = Stub(SslSettings)
        def socketSettings = Stub(SocketSettings)
        def serverSettings = Stub(ServerSettings)
        def heartbeatSocketSettings = Stub(SocketSettings)
        def credentialList = [MongoCredential.createMongoX509Credential('test')]
        def connectionPoolSettings = Stub(ConnectionPoolSettings)
        def codecRegistry = Stub(CodecRegistry)
        def commandListener = Stub(CommandListener)
        def clusterSettings = ClusterSettings.builder().hosts([new ServerAddress('localhost')]).requiredReplicaSetName('test').build()
        def compressorList = [MongoCompressor.createZlibCompressor()]

        settings = MongoClientSettings.builder()
                .readPreference(ReadPreference.secondary())
                .writeConcern(WriteConcern.JOURNALED)
                .retryWrites(true)
                .readConcern(ReadConcern.LOCAL)
                .applicationName('app1')
                .addCommandListener(commandListener)
                .sslSettings(sslSettings)
                .socketSettings(socketSettings)
                .serverSettings(serverSettings)
                .heartbeatSocketSettings(heartbeatSocketSettings)
                .credentialList(credentialList)
                .connectionPoolSettings(connectionPoolSettings)
                .codecRegistry(codecRegistry)
                .clusterSettings(clusterSettings)
                .compressorList(compressorList)
                .build()

        then:
        expect settings, isTheSameAs(MongoClientSettings.builder(settings).build())
    }

    def 'applicationName can be 128 bytes when encoded as UTF-8'() {
        given:
        def applicationName = 'a' * 126 + '\u00A0'

        when:
        def settings = MongoClientSettings.builder().applicationName(applicationName).build()

        then:
        settings.applicationName == applicationName
    }

    def 'should throw IllegalArgumentException if applicationName exceeds 128 bytes when encoded as UTF-8'() {
        given:
        def applicationName = 'a' * 127 + '\u00A0'

        when:
        MongoClientSettings.builder().applicationName(applicationName)

        then:
        thrown(IllegalArgumentException)
    }


    def 'should add command listeners'() {
        given:
        CommandListener commandListenerOne = Mock(CommandListener)
        CommandListener commandListenerTwo = Mock(CommandListener)
        CommandListener commandListenerThree = Mock(CommandListener)

        when:
        def settings = MongoClientSettings.builder().build()

        then:
        settings.commandListeners.size() == 0

        when:
        settings = MongoClientSettings.builder()
                .addCommandListener(commandListenerOne)
                .build()

        then:
        settings.commandListeners.size() == 1
        settings.commandListeners[0].is commandListenerOne

        when:
        settings = MongoClientSettings.builder()
                .addCommandListener(commandListenerOne)
                .addCommandListener(commandListenerTwo)
                .build()

        then:
        settings.commandListeners.size() == 2
        settings.commandListeners[0].is commandListenerOne
        settings.commandListeners[1].is commandListenerTwo

        when:
        def copiedsettings = MongoClientSettings.builder(settings).addCommandListener(commandListenerThree).build()

        then:
        copiedsettings.commandListeners.size() == 3
        copiedsettings.commandListeners[0].is commandListenerOne
        copiedsettings.commandListeners[1].is commandListenerTwo
        copiedsettings.commandListeners[2].is commandListenerThree
        settings.commandListeners.size() == 2
        settings.commandListeners[0].is commandListenerOne
        settings.commandListeners[1].is commandListenerTwo
    }


    def 'should only have the following methods in the builder'() {
        when:
        // A regression test so that if anymore methods are added then the builder(final MongoClientSettings settings) should be updated
        def actual = MongoClientSettings.Builder.declaredMethods.grep { !it.synthetic } *.name.sort()
        def expected = ['addCommandListener', 'applicationName', 'applyConnectionString', 'build', 'clusterSettings', 'codecRegistry',
                        'compressorList', 'connectionPoolSettings', 'credential', 'credentialList', 'heartbeatSocketSettings',
                        'readConcern', 'readPreference', 'retryWrites', 'serverSettings', 'socketSettings', 'sslSettings',
                        'streamFactoryFactory', 'writeConcern']

        then:
        actual == expected
    }
}
