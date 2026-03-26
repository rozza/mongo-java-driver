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

import com.mongodb.AwsCredential;
import com.mongodb.ClusterFixture;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoCredential;
import com.mongodb.MongoSecurityException;
import com.mongodb.ServerAddress;
import com.mongodb.async.FutureResultCallback;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.SocketSettings;
import com.mongodb.internal.authentication.AwsCredentialHelper;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.function.Supplier;

import static com.mongodb.AuthenticationMechanism.MONGODB_AWS;
import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static com.mongodb.ClusterFixture.getClusterConnectionMode;
import static com.mongodb.ClusterFixture.getConnectionString;
import static com.mongodb.ClusterFixture.getCredential;
import static com.mongodb.ClusterFixture.getSslSettings;
import static com.mongodb.connection.ClusterConnectionMode.SINGLE;
import static com.mongodb.internal.connection.CommandHelper.executeCommand;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class AwsAuthenticationSpecificationTest {

    @BeforeAll
    static void beforeAll() {
        assumeTrue(getCredential() != null && getCredential().getAuthenticationMechanism() == MONGODB_AWS);

        String providerProperty = System.getProperty("org.mongodb.test.aws.credential.provider", "awsSdkV2");
        switch (providerProperty) {
            case "builtIn":
                AwsCredentialHelper.requireBuiltInProvider();
                break;
            case "awsSdkV1":
                AwsCredentialHelper.requireAwsSdkV1Provider();
                break;
            case "awsSdkV2":
                AwsCredentialHelper.requireAwsSdkV2Provider();
                break;
            default:
                throw new IllegalArgumentException("Unrecognized AWS credential provider: " + providerProperty);
        }
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
    void shouldNotAuthorizeWhenNotAuthenticatedAsync() {
        InternalStreamConnection connection = createConnection(true, null);
        try {
            openConnection(connection, true);
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
    void shouldAuthorizeWhenSuccessfullyAuthenticatedAsync() {
        InternalStreamConnection connection = createConnection(true, getCredential());
        try {
            openConnection(connection, true);
            executeCommand(getConnectionString().getDatabase(),
                    new BsonDocument("count", new BsonString("test")),
                    getClusterConnectionMode(), null, connection, OPERATION_CONTEXT);
        } finally {
            connection.close();
        }
    }

    @Test
    void shouldNotAuthenticateWhenProviderGivesInvalidSessionTokenSync() {
        assumeTrue(System.getenv("AWS_SESSION_TOKEN") != null && !System.getenv("AWS_SESSION_TOKEN").isEmpty());

        InternalStreamConnection connection = createConnection(false,
                getCredential().withMechanismProperty(MongoCredential.AWS_CREDENTIAL_PROVIDER_KEY,
                        (Supplier<AwsCredential>) () -> new AwsCredential(
                                System.getenv("AWS_ACCESS_KEY_ID"),
                                System.getenv("AWS_SECRET_ACCESS_KEY"),
                                "fake-session-token")));
        try {
            assertThrows(MongoSecurityException.class, () -> openConnection(connection, false));
        } finally {
            connection.close();
        }
    }

    private static InternalStreamConnection createConnection(final boolean async, final MongoCredential credential) {
        return new InternalStreamConnection(SINGLE,
                new ServerId(new ClusterId(), new ServerAddress(getConnectionString().getHosts().get(0))),
                new TestConnectionGenerationSupplier(),
                async ? new AsynchronousSocketChannelStreamFactory(new DefaultInetAddressResolver(),
                        SocketSettings.builder().build(), getSslSettings())
                        : new SocketStreamFactory(new DefaultInetAddressResolver(),
                        SocketSettings.builder().build(), getSslSettings()),
                Collections.emptyList(), null,
                new InternalStreamConnectionInitializer(SINGLE, createAuthenticator(credential),
                        null, Collections.emptyList(), null));
    }

    private static Authenticator createAuthenticator(final MongoCredential credential) {
        return credential == null ? null : new AwsAuthenticator(new MongoCredentialWithCache(credential), SINGLE, null);
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
