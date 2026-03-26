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

import com.mongodb.MongoSecurityException;
import com.mongodb.ServerAddress;
import com.mongodb.async.FutureResultCallback;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.ServerType;
import com.mongodb.internal.TimeoutSettings;
import org.bson.BsonDocument;
import org.bson.io.BsonInput;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.security.sasl.SaslException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.mongodb.MongoCredential.createScramSha1Credential;
import static com.mongodb.MongoCredential.createScramSha256Credential;
import static com.mongodb.connection.ClusterConnectionMode.SINGLE;
import static com.mongodb.internal.connection.MessageHelper.buildSuccessfulReply;
import static com.mongodb.internal.connection.OperationContext.simpleOperationContext;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ScramShaAuthenticatorTest {

    private final ServerId serverId = new ServerId(new ClusterId(), new ServerAddress("localhost", 27017));
    private final ConnectionDescription connectionDescription = new ConnectionDescription(serverId);
    private final OperationContext operationContext = simpleOperationContext(TimeoutSettings.DEFAULT, null);
    private static final MongoCredentialWithCache SHA1_CREDENTIAL =
            new MongoCredentialWithCache(createScramSha1Credential("user", "database", "pencil".toCharArray()));
    private static final MongoCredentialWithCache SHA256_CREDENTIAL =
            new MongoCredentialWithCache(createScramSha256Credential("user", "database", "pencil".toCharArray()));

    static Stream<Object[]> sha1RfcCombinations() {
        List<Object[]> combos = new ArrayList<>();
        for (boolean async : new boolean[]{true, false}) {
            for (boolean emptyExchange : new boolean[]{true, false}) {
                combos.add(new Object[]{async, emptyExchange});
            }
        }
        return combos.stream();
    }

    @ParameterizedTest
    @MethodSource("sha1RfcCombinations")
    void shouldSuccessfullyAuthenticateWithSha1AsPerRFCSpec(boolean async, boolean emptyExchange) {
        String payloads = "\n"
                + "            C: n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL\n"
                + "            S: r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096\n"
                + "            C: c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=\n"
                + "            S: v=rmF9pqV8S7suAoZWja4dJRkFsKQ=\n";

        MongoCredentialWithCache credential = new MongoCredentialWithCache(
                createScramSha1Credential("user", "database", "pencil".toCharArray()));
        ScramShaAuthenticator authenticator = new ScramShaAuthenticator(credential,
                (ScramShaAuthenticator.RandomStringGenerator) (length) -> "fyko+d2lbbFgONRv9qkxdawL",
                (ScramShaAuthenticator.AuthenticationHashGenerator) (cred) -> "pencil", SINGLE, null);

        validateAuthentication(payloads, authenticator, async, emptyExchange);
    }

    @ParameterizedTest
    @MethodSource("sha1RfcCombinations")
    void shouldSuccessfullyAuthenticateWithSha256AsPerRFCSpec(boolean async, boolean emptyExchange) {
        String payloads = "\n"
                + "            C: n,,n=user,r=rOprNGfwEbeRWgbNEkqO\n"
                + "            S: r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096\n"
                + "            C: c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=\n"
                + "            S: v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=\n";

        MongoCredentialWithCache credential = new MongoCredentialWithCache(
                createScramSha256Credential("user", "database", "pencil".toCharArray()));
        ScramShaAuthenticator authenticator = new ScramShaAuthenticator(credential,
                (ScramShaAuthenticator.RandomStringGenerator) (length) -> "rOprNGfwEbeRWgbNEkqO",
                (ScramShaAuthenticator.AuthenticationHashGenerator) (cred) -> "pencil", SINGLE, null);

        validateAuthentication(payloads, authenticator, async, emptyExchange);
    }

    static Stream<Object[]> invalidRValueCombinations() {
        List<Object[]> combos = new ArrayList<>();
        for (boolean async : new boolean[]{true, false}) {
            for (MongoCredentialWithCache credential : new MongoCredentialWithCache[]{SHA1_CREDENTIAL, SHA256_CREDENTIAL}) {
                combos.add(new Object[]{async, credential});
            }
        }
        return combos.stream();
    }

    @ParameterizedTest
    @MethodSource("invalidRValueCombinations")
    void shouldThrowIfInvalidRValueFromServer(boolean async, MongoCredentialWithCache credential) {
        List<String> serverResponses = Arrays.asList("r=InvalidRValue,s=MYSALT,i=4096");
        ScramShaAuthenticator authenticator = new ScramShaAuthenticator(credential,
                (ScramShaAuthenticator.RandomStringGenerator) (length) -> "rOprNGfwEbeRWgbNEkqO",
                (ScramShaAuthenticator.AuthenticationHashGenerator) (cred) -> "pencil", SINGLE, null);

        MongoSecurityException e = assertThrows(MongoSecurityException.class, () ->
                authenticate(createConnection(serverResponses), authenticator, async));
        assertInstanceOf(SaslException.class, e.getCause());
        assertEquals("Server sent an invalid nonce.", e.getCause().getMessage());
    }

    @ParameterizedTest
    @MethodSource("invalidRValueCombinations")
    void shouldThrowIfIterationCountIsBelowMinimum(boolean async, MongoCredentialWithCache credential) {
        List<List<String>> messages = createMessages(
                "S: r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=QSXCR+Q6sek8bf92,i=4095");
        List<String> serverResponses = messages.get(1);
        ScramShaAuthenticator authenticator = new ScramShaAuthenticator(credential,
                (ScramShaAuthenticator.RandomStringGenerator) (length) -> "rOprNGfwEbeRWgbNEkqO",
                (ScramShaAuthenticator.AuthenticationHashGenerator) (cred) -> "pencil", SINGLE, null);

        MongoSecurityException e = assertThrows(MongoSecurityException.class, () ->
                authenticate(createConnection(serverResponses), authenticator, async));
        assertInstanceOf(SaslException.class, e.getCause());
        assertEquals("Invalid iteration count.", e.getCause().getMessage());
    }

    @ParameterizedTest
    @MethodSource("invalidRValueCombinations")
    void shouldThrowIfInvalidServerSignature(boolean async, MongoCredentialWithCache credential) {
        List<List<String>> messages = createMessages(
                "S: r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=QSXCR+Q6sek8bf92,i=4096\n"
                        + "            S: v=InvalidServerSignature");
        List<String> serverResponses = messages.get(1);
        ScramShaAuthenticator authenticator = new ScramShaAuthenticator(credential,
                (ScramShaAuthenticator.RandomStringGenerator) (length) -> "rOprNGfwEbeRWgbNEkqO",
                (ScramShaAuthenticator.AuthenticationHashGenerator) (cred) -> "pencil", SINGLE, null);

        MongoSecurityException e = assertThrows(MongoSecurityException.class, () ->
                authenticate(createConnection(serverResponses), authenticator, async));
        assertInstanceOf(SaslException.class, e.getCause());
        assertEquals("Server signature was invalid.", e.getCause().getMessage());
    }

    static Stream<Boolean> asyncProvider() {
        return Stream.of(true, false);
    }

    @ParameterizedTest
    @MethodSource("asyncProvider")
    void shouldThrowIfTooManyStepsSha1(boolean async) {
        List<List<String>> messages = createMessages(
                "S: r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096\n"
                        + "            S: v=rmF9pqV8S7suAoZWja4dJRkFsKQ=\n"
                        + "            S: z=ExtraStep");
        List<String> serverResponses = messages.get(1);
        ScramShaAuthenticator authenticator = new ScramShaAuthenticator(SHA1_CREDENTIAL,
                (ScramShaAuthenticator.RandomStringGenerator) (length) -> "fyko+d2lbbFgONRv9qkxdawL",
                (ScramShaAuthenticator.AuthenticationHashGenerator) (cred) -> "pencil", SINGLE, null);

        MongoSecurityException e = assertThrows(MongoSecurityException.class, () ->
                authenticate(createConnection(serverResponses), authenticator, async));
        assertInstanceOf(SaslException.class, e.getCause());
        assertEquals("Too many steps involved in the SCRAM-SHA-1 negotiation.", e.getCause().getMessage());
    }

    @ParameterizedTest
    @MethodSource("asyncProvider")
    void shouldThrowIfTooManyStepsSha256(boolean async) {
        List<List<String>> messages = createMessages(
                "S: r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096\n"
                        + "            S: v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=\n"
                        + "            S: z=ExtraStep", true);
        List<String> serverResponses = messages.get(1);
        ScramShaAuthenticator authenticator = new ScramShaAuthenticator(SHA256_CREDENTIAL,
                (ScramShaAuthenticator.RandomStringGenerator) (length) -> "rOprNGfwEbeRWgbNEkqO",
                (ScramShaAuthenticator.AuthenticationHashGenerator) (cred) -> "pencil", SINGLE, null);

        MongoSecurityException e = assertThrows(MongoSecurityException.class, () ->
                authenticate(createConnection(serverResponses), authenticator, async));
        assertInstanceOf(SaslException.class, e.getCause());
        assertEquals("Too many steps involved in the SCRAM-SHA-256 negotiation.", e.getCause().getMessage());
    }

    @ParameterizedTest
    @MethodSource("asyncProvider")
    void shouldCompleteAuthenticationWhenDoneIsSetToTruePrematurely(boolean async) {
        List<List<String>> messages = createMessages(
                "S: r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096\n"
                        + "            S: v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=");
        List<String> serverResponses = messages.get(1);

        // server sends done=true on first response, client is not complete after processing response
        ScramShaAuthenticator authenticator1 = new ScramShaAuthenticator(SHA256_CREDENTIAL,
                (ScramShaAuthenticator.RandomStringGenerator) (length) -> "rOprNGfwEbeRWgbNEkqO",
                (ScramShaAuthenticator.AuthenticationHashGenerator) (cred) -> "pencil", SINGLE, null);
        MongoSecurityException e = assertThrows(MongoSecurityException.class, () ->
                authenticate(createConnection(serverResponses, 0), authenticator1, async));
        assertTrue(e.getMessage().contains("server completed challenges before client completed responses"));

        // server sends done=true on second response, client is complete after processing response -- should succeed
        ScramShaAuthenticator authenticator2 = new ScramShaAuthenticator(SHA256_CREDENTIAL,
                (ScramShaAuthenticator.RandomStringGenerator) (length) -> "rOprNGfwEbeRWgbNEkqO",
                (ScramShaAuthenticator.AuthenticationHashGenerator) (cred) -> "pencil", SINGLE, null);
        authenticate(createConnection(serverResponses, 1), authenticator2, async);
    }

    @ParameterizedTest
    @MethodSource("asyncProvider")
    void shouldThrowExceptionWhenDoneIsSetToTruePrematurelyAndServerResponseIsInvalid(boolean async) {
        List<List<String>> messages = createMessages(
                "S: r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096\n"
                        + "            S: v=invalidResponse");
        List<String> serverResponses = messages.get(1);

        ScramShaAuthenticator authenticator = new ScramShaAuthenticator(SHA256_CREDENTIAL,
                (ScramShaAuthenticator.RandomStringGenerator) (length) -> "rOprNGfwEbeRWgbNEkqO",
                (ScramShaAuthenticator.AuthenticationHashGenerator) (cred) -> "pencil", SINGLE, null);
        MongoSecurityException e = assertThrows(MongoSecurityException.class, () ->
                authenticate(createConnection(serverResponses, 1), authenticator, async));
        assertInstanceOf(SaslException.class, e.getCause());
        assertEquals("Server signature was invalid.", e.getCause().getMessage());
    }

    private TestInternalConnection createConnection(List<String> serverResponses) {
        return createConnection(serverResponses, -1);
    }

    private TestInternalConnection createConnection(List<String> serverResponses, int responseWhereDoneIsTrue) {
        TestInternalConnection connection = new TestInternalConnection(serverId, ServerType.STANDALONE);
        for (int index = 0; index < serverResponses.size(); index++) {
            boolean isDone = (index == responseWhereDoneIsTrue);
            connection.enqueueReply(
                    buildSuccessfulReply("{conversationId: 1, payload: BinData(0, '"
                            + encode64(serverResponses.get(index)) + "'), done: " + isDone + ", ok: 1}"));
        }
        if (responseWhereDoneIsTrue < 0) {
            connection.enqueueReply(buildSuccessfulReply("{conversationId: 1, done: true, ok: 1}"));
        }
        return connection;
    }

    private void validateAuthentication(String payloads, ScramShaAuthenticator authenticator, boolean async,
                                        boolean emptyExchange) {
        List<List<String>> messages = createMessages(payloads, emptyExchange);
        List<String> clientMessages = messages.get(0);
        List<String> serverResponses = messages.get(1);
        TestInternalConnection connection = createConnection(serverResponses, emptyExchange ? -1 : 1);
        authenticate(connection, authenticator, async);
        validateClientMessages(connection, clientMessages, authenticator.getMechanismName());
    }

    private void validateClientMessages(TestInternalConnection connection, List<String> clientMessages,
                                        String mechanism) {
        validateClientMessages(connection, clientMessages, mechanism, false);
    }

    private void validateClientMessages(TestInternalConnection connection, List<String> clientMessages,
                                        String mechanism, boolean speculativeAuthenticate) {
        List<BsonDocument> sent = new ArrayList<>();
        for (BsonInput bsonInput : connection.getSent()) {
            sent.add(MessageHelper.decodeCommand(bsonInput));
        }
        assertEquals(clientMessages.size(), sent.size());
        for (int i = 0; i < sent.size(); i++) {
            BsonDocument sentMessage = sent.get(i);
            String messageStart = speculativeAuthenticate || i != 0
                    ? "saslContinue: 1, conversationId: 1"
                    : "saslStart: 1, mechanism:'" + mechanism + "', options: {skipEmptyExchange: true}";
            BsonDocument expectedMessage = BsonDocument.parse(
                    "{" + messageStart + ", payload: BinData(0, '" + encode64(clientMessages.get(i)) + "'), $db: \"database\"}");
            assertEquals(expectedMessage, sentMessage);
        }
    }

    private void authenticate(TestInternalConnection connection, ScramShaAuthenticator authenticator, boolean async) {
        if (async) {
            FutureResultCallback<Void> futureCallback = new FutureResultCallback<>();
            authenticator.authenticateAsync(connection, connectionDescription, operationContext, futureCallback);
            futureCallback.get(5, TimeUnit.SECONDS);
        } else {
            authenticator.authenticate(connection, connectionDescription, operationContext);
        }
    }

    private String encode64(String string) {
        return Base64.getEncoder().encodeToString(string.getBytes(StandardCharsets.UTF_8));
    }

    private List<List<String>> createMessages(String messages) {
        return createMessages(messages, true);
    }

    private List<List<String>> createMessages(String messages, boolean emptyExchange) {
        List<String> clientMessages = new ArrayList<>();
        List<String> serverResponses = new ArrayList<>();
        String[] lines = messages.split("\n");
        for (String line : lines) {
            String trimmed = line.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            String type = trimmed.substring(0, 2);
            String message = trimmed.substring(2).trim();

            if ("C:".equals(type)) {
                clientMessages.add(message);
            } else if ("S:".equals(type)) {
                serverResponses.add(message);
            } else {
                throw new IllegalArgumentException("Invalid message: " + message);
            }
        }
        if (emptyExchange) {
            clientMessages.add("");
        }
        List<List<String>> result = new ArrayList<>();
        result.add(clientMessages);
        result.add(serverResponses);
        return result;
    }
}
