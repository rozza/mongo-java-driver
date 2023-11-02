package com.mongodb.internal.connection

import com.mongodb.MongoSocketException
import com.mongodb.MongoSocketOpenException
import com.mongodb.ServerAddress
import com.mongodb.connection.AsyncCompletionHandler
import com.mongodb.connection.AsynchronousSocketChannelStreamFactoryFactory
import com.mongodb.connection.SocketSettings
import com.mongodb.connection.SslSettings
import spock.lang.IgnoreIf
import spock.lang.Specification
import util.spock.annotations.Slow

import java.nio.channels.AsynchronousChannelGroup
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT
import static com.mongodb.ClusterFixture.getSslSettings
import static java.util.concurrent.TimeUnit.MILLISECONDS

class AsyncSocketChannelStreamSpecification extends Specification {

    @Slow
    @IgnoreIf({ getSslSettings().isEnabled() })
    def 'should successfully connect with working ip address list'() {
        given:
        def port = 27017
        def socketSettings = SocketSettings.builder().connectTimeout(100, MILLISECONDS).build()
        def sslSettings = SslSettings.builder().build()
        def channelGroup = AsynchronousChannelGroup.withThreadPool(Executors.newFixedThreadPool(5))
        def factoryFactory = AsynchronousSocketChannelStreamFactoryFactory.builder().group(channelGroup).build()
        def factory = factoryFactory.create(socketSettings, sslSettings)
        def inetAddresses = [new InetSocketAddress(InetAddress.getByName('192.168.255.255'), port),
                             new InetSocketAddress(InetAddress.getByName('127.0.0.1'), port)]

        def serverAddress = Stub(ServerAddress)
        serverAddress.getSocketAddresses() >> inetAddresses

        def stream = factory.create(serverAddress)

        when:
        stream.open(OPERATION_CONTEXT)

        then:
        !stream.isClosed()
    }

    @Slow
    @IgnoreIf({ getSslSettings().isEnabled() })
    def 'should fail to connect with non-working ip address list'() {
        given:
        def port = 27017
        def socketSettings = SocketSettings.builder().connectTimeout(100, MILLISECONDS).build()
        def sslSettings = SslSettings.builder().build()
        def factoryFactory = AsynchronousSocketChannelStreamFactoryFactory.builder()
                .group(AsynchronousChannelGroup.withThreadPool(Executors.newFixedThreadPool(5)))
                .build()

        def factory = factoryFactory.create(socketSettings, sslSettings)

        def inetAddresses = [new InetSocketAddress(InetAddress.getByName('192.168.255.255'), port),
                             new InetSocketAddress(InetAddress.getByName('1.2.3.4'), port)]

        def serverAddress = Stub(ServerAddress)
        serverAddress.getSocketAddresses() >> inetAddresses

        def stream = factory.create(serverAddress)

        when:
        stream.open(OPERATION_CONTEXT)

        then:
        thrown(MongoSocketOpenException)
    }

    @IgnoreIf({ getSslSettings().isEnabled() })
    def 'should fail AsyncCompletionHandler if name resolution fails'() {
        given:
        def serverAddress = Stub(ServerAddress)
        def exception = new MongoSocketException('Temporary failure in name resolution', serverAddress)
        serverAddress.getSocketAddresses() >> { throw exception }

        def stream = new AsynchronousSocketChannelStream(serverAddress,
                SocketSettings.builder().connectTimeout(100, MILLISECONDS).build(),
                new PowerOfTwoBufferPool(), null)
        def callback = new CallbackErrorHolder()

        when:
        stream.openAsync(OPERATION_CONTEXT, callback)

        then:
        callback.getError().is(exception)
    }

    class CallbackErrorHolder implements AsyncCompletionHandler<Void> {
        CountDownLatch latch = new CountDownLatch(1)
        Throwable throwable = null

        Throwable getError() {
            latch.countDown()
            throwable
        }

        @Override
        void completed(Void r) {
            latch.await()
        }

        @Override
        void failed(Throwable t) {
            throwable = t
            latch.countDown()
        }
    }
}
