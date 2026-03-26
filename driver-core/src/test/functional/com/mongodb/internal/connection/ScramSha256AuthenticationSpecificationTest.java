/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.internal.connection;

import com.mongodb.ClusterFixture;
import com.mongodb.MongoCredential;
import com.mongodb.MongoSecurityException;
import com.mongodb.ReadPreference;
import com.mongodb.async.FutureResultCallback;
import com.mongodb.internal.binding.AsyncClusterBinding;
import com.mongodb.internal.binding.ClusterBinding;
import com.mongodb.internal.binding.ReadWriteBinding;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.operation.CommandReadOperation;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DocumentCodec;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static com.mongodb.ClusterFixture.createAsyncCluster;
import static com.mongodb.ClusterFixture.createCluster;
import static com.mongodb.ClusterFixture.getBinding;
import static com.mongodb.ClusterFixture.isAuthenticated;
import static com.mongodb.MongoCredential.createCredential;
import static com.mongodb.MongoCredential.createScramSha1Credential;
import static com.mongodb.MongoCredential.createScramSha256Credential;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class ScramSha256AuthenticationSpecificationTest {

    private static final MongoCredential SHA1_IMPLICIT = createCredential("sha1", "admin", "sha1".toCharArray());
    private static final MongoCredential SHA1_EXPLICIT = createScramSha1Credential("sha1", "admin", "sha1".toCharArray());
    private static final MongoCredential SHA256_IMPLICIT = createCredential("sha256", "admin", "sha256".toCharArray());
    private static final MongoCredential SHA256_EXPLICIT = createScramSha256Credential("sha256", "admin", "sha256".toCharArray());
    private static final MongoCredential BOTH_IMPLICIT = createCredential("both", "admin", "both".toCharArray());
    private static final MongoCredential BOTH_EXPLICIT_SHA1 = createScramSha1Credential("both", "admin", "both".toCharArray());
    private static final MongoCredential BOTH_EXPLICIT_SHA256 = createScramSha256Credential("both", "admin", "both".toCharArray());
    private static final MongoCredential SHA1_AS_SHA256 = createScramSha256Credential("sha1", "admin", "sha1".toCharArray());
    private static final MongoCredential SHA256_AS_SHA1 = createScramSha1Credential("sha256", "admin", "sha256".toCharArray());
    private static final MongoCredential NON_EXISTENT_USER = createCredential("nonexistent", "admin", "pwd".toCharArray());
    private static final MongoCredential USER_NINE_PREPPED = createScramSha256Credential("IX", "admin", "IX".toCharArray());
    private static final MongoCredential USER_NINE_UNPREPPED = createScramSha256Credential("IX", "admin", "I\u00ADX".toCharArray());
    private static final MongoCredential USER_FOUR_PREPPED = createScramSha256Credential("\u2168", "admin", "IV".toCharArray());
    private static final MongoCredential USER_FOUR_UNPREPPED = createScramSha256Credential("\u2168", "admin", "I\u00ADV".toCharArray());

    @BeforeAll
    static void setupSpec() {
        assumeTrue(isAuthenticated());
        createUser("sha1", "sha1", Arrays.asList("SCRAM-SHA-1"));
        createUser("sha256", "sha256", Arrays.asList("SCRAM-SHA-256"));
        createUser("both", "both", Arrays.asList("SCRAM-SHA-1", "SCRAM-SHA-256"));
        createUser("IX", "IX", Arrays.asList("SCRAM-SHA-256"));
        createUser("\u2168", "\u2163", Arrays.asList("SCRAM-SHA-256"));
    }

    @AfterAll
    static void cleanupSpec() {
        if (!isAuthenticated()) {
            return;
        }
        dropUser("sha1");
        dropUser("sha256");
        dropUser("both");
        dropUser("IX");
        dropUser("\u2168");
    }

    static Stream<MongoCredential> validCredentials() {
        return Stream.of(SHA1_IMPLICIT, SHA1_EXPLICIT, SHA256_IMPLICIT, SHA256_EXPLICIT,
                BOTH_IMPLICIT, BOTH_EXPLICIT_SHA1, BOTH_EXPLICIT_SHA256);
    }

    static Stream<MongoCredential> invalidCredentials() {
        return Stream.of(SHA1_AS_SHA256, SHA256_AS_SHA1, NON_EXISTENT_USER);
    }

    static Stream<MongoCredential> saslPrepCredentials() {
        return Stream.of(USER_NINE_PREPPED, USER_NINE_UNPREPPED, USER_FOUR_PREPPED, USER_FOUR_UNPREPPED);
    }

    @ParameterizedTest
    @MethodSource("validCredentials")
    void shouldAuthenticateAndAuthorize(final MongoCredential credential) {
        Cluster cluster = createCluster(credential);
        try {
            assertDoesNotThrow(() ->
                    new CommandReadOperation<Document>("admin",
                            new BsonDocumentWrapper<>(new Document("dbstats", 1), new DocumentCodec()),
                            new DocumentCodec())
                            .execute(new ClusterBinding(cluster, ReadPreference.primary()), OPERATION_CONTEXT));
        } finally {
            cluster.close();
        }
    }

    @ParameterizedTest
    @MethodSource("invalidCredentials")
    void shouldFailAuthenticationWithWrongMechanism(final MongoCredential credential) {
        Cluster cluster = createCluster(credential);
        try {
            assertThrows(MongoSecurityException.class, () ->
                    new CommandReadOperation<Document>("admin",
                            new BsonDocumentWrapper<>(new Document("dbstats", 1), new DocumentCodec()),
                            new DocumentCodec())
                            .execute(new ClusterBinding(cluster, ReadPreference.primary()), OPERATION_CONTEXT));
        } finally {
            cluster.close();
        }
    }

    @ParameterizedTest
    @MethodSource("saslPrepCredentials")
    void shouldHandleSaslPrep(final MongoCredential credential) {
        Cluster cluster = createCluster(credential);
        try {
            assertDoesNotThrow(() ->
                    new CommandReadOperation<Document>("admin",
                            new BsonDocumentWrapper<>(new Document("dbstats", 1), new DocumentCodec()),
                            new DocumentCodec())
                            .execute(new ClusterBinding(cluster, ReadPreference.primary()), OPERATION_CONTEXT));
        } finally {
            cluster.close();
        }
    }

    private static void createUser(final String userName, final String password, final List<String> mechanisms) {
        Document createUserCommand = new Document("createUser", userName)
                .append("pwd", password)
                .append("roles", Arrays.asList("root"))
                .append("mechanisms", mechanisms);
        ReadWriteBinding binding = getBinding();
        new CommandReadOperation<>("admin",
                new BsonDocumentWrapper<>(createUserCommand, new DocumentCodec()),
                new DocumentCodec())
                .execute(binding, ClusterFixture.getOperationContext(binding.getReadPreference()));
    }

    private static void dropUser(final String userName) {
        try {
            ReadWriteBinding binding = getBinding();
            new CommandReadOperation<>("admin",
                    new BsonDocument("dropUser", new BsonString(userName)),
                    new BsonDocumentCodec())
                    .execute(binding, ClusterFixture.getOperationContext(binding.getReadPreference()));
        } catch (Exception e) {
            // ignore
        }
    }
}
