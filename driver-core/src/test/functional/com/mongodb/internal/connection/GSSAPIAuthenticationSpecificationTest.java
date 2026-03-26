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

import com.mongodb.ClusterFixture;
import com.mongodb.KerberosSubjectProvider;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoCredential;
import com.mongodb.MongoSecurityException;
import com.mongodb.ServerAddress;
import com.mongodb.async.FutureResultCallback;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.SocketSettings;
import com.mongodb.internal.connection.netty.NettyStreamFactory;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import java.util.Collections;

import static com.mongodb.AuthenticationMechanism.GSSAPI;
import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static com.mongodb.ClusterFixture.getClusterConnectionMode;
import static com.mongodb.ClusterFixture.getConnectionString;
import static com.mongodb.ClusterFixture.getCredential;
import static com.mongodb.ClusterFixture.getLoginContextName;
import static com.mongodb.ClusterFixture.getSslSettings;
import static com.mongodb.MongoCredential.JAVA_SUBJECT_PROVIDER_KEY;
import static com.mongodb.MongoCredential.createGSSAPICredential;
import static com.mongodb.connection.ClusterConnectionMode.SINGLE;
import static com.mongodb.internal.connection.CommandHelper.executeCommand;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class GSSAPIAuthenticationSpecificationTest {

    @BeforeAll
    static void beforeAll() {
        assumeTrue(getCredential() != null && getCredential().getAuthenticationMechanism() == GSSAPI);
    }

    @Test
    void shouldNotAuthorizeWhenNotAuthenticatedSync() {
        InternalStreamConnection connection = createConnection(false, null);
        try {
            openConnection(connection, false);
            assertThrows(MongoCommandException.class, () ->
                    executeCommand(getConnectionString().getDatabase(),
                            new BsonDocument("count", new BsonString("test")),
                            getClusterConnectionMode(), null, connection, OPERATION_CONTEXT));
        } finally {
            connection.close();
        }
    }

    @Test
    void shouldAuthorizeWhenSuccessfullyAuthenticatedSync() {
        InternalStreamConnection connection = createConnection(false, getCredential());
        try {
            openConnection(connection, false);
            executeCommand(getConnectionString().getDatabase(),
                    new BsonDocument("count", new BsonString("test")),
                    getClusterConnectionMode(), null, connection, OPERATION_CONTEXT);
        } finally {
            connection.close();
        }
    }

    @Test
    void shouldAuthorizeWithSubjectProviderSync() {
        MongoCredential credential = getCredential()
                .withMechanismProperty(JAVA_SUBJECT_PROVIDER_KEY, new KerberosSubjectProvider());
        InternalStreamConnection connection = createConnection(false, credential);
        try {
            openConnection(connection, false);
            executeCommand(getConnectionString().getDatabase(),
                    new BsonDocument("count", new BsonString("test")),
                    getClusterConnectionMode(), null, connection, OPERATION_CONTEXT);
        } finally {
            connection.close();
        }
    }

    @Test
    void shouldThrowMongoSecurityExceptionWhenAuthenticationFailsSync() {
        InternalStreamConnection connection = createConnection(false, createGSSAPICredential("wrongUserName"));
        try {
            assertThrows(MongoSecurityException.class, () -> {
                openConnection(connection, false);
                executeCommand(getConnectionString().getDatabase(),
                        new BsonDocument("count", new BsonString("test")),
                        getClusterConnectionMode(), null, connection, OPERATION_CONTEXT);
            });
        } finally {
            connection.close();
        }
    }

    @Test
    void shouldAuthorizeWithSubjectPropertySync() throws Exception {
        LoginContext loginContext = new LoginContext(getLoginContextName());
        loginContext.login();
        Subject subject = loginContext.getSubject();
        assertNotNull(subject);

        MongoCredential credential = getCredential()
                .withMechanismProperty(MongoCredential.JAVA_SUBJECT_KEY, subject);
        InternalStreamConnection connection = createConnection(false, credential);
        try {
            openConnection(connection, false);
            executeCommand(getConnectionString().getDatabase(),
                    new BsonDocument("count", new BsonString("test")),
                    getClusterConnectionMode(), null, connection, OPERATION_CONTEXT);
        } finally {
            connection.close();
        }
    }

    private static InternalStreamConnection createConnection(final boolean async, final MongoCredential credential) {
        return new InternalStreamConnection(SINGLE,
                new ServerId(new ClusterId(), new ServerAddress(getConnectionString().getHosts().get(0))),
                new TestConnectionGenerationSupplier(),
                async ? new NettyStreamFactory(SocketSettings.builder().build(), getSslSettings())
                        : new SocketStreamFactory(new DefaultInetAddressResolver(),
                        SocketSettings.builder().build(), getSslSettings()),
                Collections.emptyList(), null,
                new InternalStreamConnectionInitializer(SINGLE, createAuthenticator(credential),
                        null, Collections.emptyList(), null));
    }

    private static Authenticator createAuthenticator(final MongoCredential credential) {
        return credential == null ? null
                : new GSSAPIAuthenticator(new MongoCredentialWithCache(credential), getClusterConnectionMode(), null);
    }

    private static void openConnection(final InternalConnection connection, final boolean async) {
        if (async) {
            FutureResultCallback<Void> futureResultCallback = new FutureResultCallback<>();
            connection.openAsync(OPERATION_CONTEXT, futureResultCallback);
            try {
                futureResultCallback.get(ClusterFixture.TIMEOUT, SECONDS);
            } catch (Throwable t) {
                if (t instanceof RuntimeException) {
                    throw (RuntimeException) t;
                }
                throw new RuntimeException(t);
            }
        } else {
            connection.open(OPERATION_CONTEXT);
        }
    }
}
