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

package com.mongodb;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.mongodb.AuthenticationMechanism.PLAIN;
import static com.mongodb.AuthenticationMechanism.SCRAM_SHA_1;
import static com.mongodb.AuthenticationMechanism.SCRAM_SHA_256;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MongoCredentialTest {

    @Test
    @DisplayName("creating a credential with an unspecified mechanism should populate correct fields")
    void unspecifiedMechanismShouldPopulateCorrectFields() {
        String userName = "user";
        String database = "test";
        char[] password = "pwd".toCharArray();

        MongoCredential credential = MongoCredential.createCredential(userName, database, password);

        assertEquals(userName, credential.getUserName());
        assertEquals(database, credential.getSource());
        assertArrayEquals(password, credential.getPassword());
        assertNull(credential.getAuthenticationMechanism());
        assertNull(credential.getMechanism());
    }

    @Test
    @DisplayName("creating a Plain credential should populate all required fields")
    void plainCredentialShouldPopulateAllFields() {
        AuthenticationMechanism mechanism = PLAIN;
        String userName = "user";
        String source = "$external";
        char[] password = "pwd".toCharArray();

        MongoCredential credential = MongoCredential.createPlainCredential(userName, source, password);

        assertEquals(userName, credential.getUserName());
        assertEquals(source, credential.getSource());
        assertArrayEquals(password, credential.getPassword());
        assertEquals(mechanism, credential.getAuthenticationMechanism());
        assertEquals(MongoCredential.PLAIN_MECHANISM, credential.getMechanism());
    }

    @Test
    @DisplayName("should throw IllegalArgumentException when a required field is not passed in")
    void shouldThrowForMissingRequiredField() {
        assertThrows(IllegalArgumentException.class,
                () -> MongoCredential.createPlainCredential(null, "$external", "pwd".toCharArray()));
        assertThrows(IllegalArgumentException.class,
                () -> MongoCredential.createPlainCredential("user", "$external", null));
        assertThrows(IllegalArgumentException.class,
                () -> MongoCredential.createPlainCredential("user", null, "pwd".toCharArray()));
    }

    @Test
    @DisplayName("creating a SCRAM_SHA_1 credential should populate all required fields")
    void scramSha1CredentialShouldPopulateAllFields() {
        AuthenticationMechanism mechanism = SCRAM_SHA_1;
        String userName = "user";
        String source = "admin";
        char[] password = "pwd".toCharArray();

        MongoCredential credential = MongoCredential.createScramSha1Credential(userName, source, password);

        assertEquals(userName, credential.getUserName());
        assertEquals(source, credential.getSource());
        assertArrayEquals(password, credential.getPassword());
        assertEquals(mechanism, credential.getAuthenticationMechanism());
        assertEquals(MongoCredential.SCRAM_SHA_1_MECHANISM, credential.getMechanism());
    }

    @Test
    @DisplayName("creating a SCRAM_SHA_256 credential should populate all required fields")
    void scramSha256CredentialShouldPopulateAllFields() {
        AuthenticationMechanism mechanism = SCRAM_SHA_256;
        String userName = "user";
        String source = "admin";
        char[] password = "pwd".toCharArray();

        MongoCredential credential = MongoCredential.createScramSha256Credential(userName, source, password);

        assertEquals(userName, credential.getUserName());
        assertEquals(source, credential.getSource());
        assertArrayEquals(password, credential.getPassword());
        assertEquals(mechanism, credential.getAuthenticationMechanism());
        assertEquals(MongoCredential.SCRAM_SHA_256_MECHANISM, credential.getMechanism());
    }

    @Test
    @DisplayName("should throw IllegalArgumentException when a required field is not passed in for the SCRAM_SHA_1 mechanism")
    void shouldThrowForMissingSHA1Field() {
        assertThrows(IllegalArgumentException.class,
                () -> MongoCredential.createScramSha1Credential(null, "admin", "pwd".toCharArray()));
        assertThrows(IllegalArgumentException.class,
                () -> MongoCredential.createScramSha1Credential("user", "admin", null));
        assertThrows(IllegalArgumentException.class,
                () -> MongoCredential.createScramSha1Credential("user", null, "pwd".toCharArray()));
    }

    @Test
    @DisplayName("creating a GSSAPI Credential should populate the correct fields")
    void gssapiCredentialShouldPopulateCorrectFields() {
        AuthenticationMechanism mechanism = AuthenticationMechanism.GSSAPI;
        String userName = "user";

        MongoCredential credential = MongoCredential.createGSSAPICredential(userName);

        assertEquals(userName, credential.getUserName());
        assertEquals("$external", credential.getSource());
        assertNull(credential.getPassword());
        assertEquals(mechanism, credential.getAuthenticationMechanism());
        assertEquals(MongoCredential.GSSAPI_MECHANISM, credential.getMechanism());
    }

    @Test
    @DisplayName("creating an X.509 Credential should populate the correct fields")
    void x509CredentialShouldPopulateCorrectFields() {
        AuthenticationMechanism mechanism = AuthenticationMechanism.MONGODB_X509;
        String userName = "user";

        MongoCredential credential = MongoCredential.createMongoX509Credential(userName);

        assertEquals(userName, credential.getUserName());
        assertEquals("$external", credential.getSource());
        assertNull(credential.getPassword());
        assertEquals(mechanism, credential.getAuthenticationMechanism());
        assertEquals(MongoCredential.MONGODB_X509_MECHANISM, credential.getMechanism());
    }

    @Test
    @DisplayName("creating an X.509 Credential without a username should populate the correct fields")
    void x509CredentialWithoutUsernameShouldPopulateCorrectFields() {
        AuthenticationMechanism mechanism = AuthenticationMechanism.MONGODB_X509;

        MongoCredential credential = MongoCredential.createMongoX509Credential();

        assertNull(credential.getUserName());
        assertEquals("$external", credential.getSource());
        assertNull(credential.getPassword());
        assertEquals(mechanism, credential.getAuthenticationMechanism());
        assertEquals(MongoCredential.MONGODB_X509_MECHANISM, credential.getMechanism());
    }

    @Test
    @DisplayName("should get default value of mechanism property when there is no mapping")
    void shouldGetDefaultMechanismProperty() {
        MongoCredential credential = MongoCredential.createGSSAPICredential("user");
        assertEquals("mongodb", credential.getMechanismProperty("unmappedKey", "mongodb"));
    }

    @Test
    @DisplayName("should get mapped mechanism properties when there is a mapping")
    void shouldGetMappedMechanismProperties() {
        String firstKey = "firstKey";
        String firstValue = "firstValue";
        String secondKey = "secondKey";
        Integer secondValue = 2;

        MongoCredential credential = MongoCredential.createGSSAPICredential("user")
                .withMechanismProperty(firstKey, firstValue);

        assertEquals(firstValue, credential.getMechanismProperty(firstKey, "default"));
        assertEquals(firstValue, credential.getMechanismProperty(firstKey.toLowerCase(), "default"));

        credential = credential.withMechanismProperty(secondKey, secondValue);

        assertEquals(firstValue, credential.getMechanismProperty(firstKey, "default"));
        assertEquals(secondValue, credential.getMechanismProperty(secondKey, 1));
    }

    @Test
    @DisplayName("should preserve other properties when adding a mechanism property")
    void shouldPreserveOtherProperties() {
        MongoCredential credential = MongoCredential.createPlainCredential("user", "source", "pwd".toCharArray());
        MongoCredential newCredential = credential.withMechanismProperty("foo", "bar");

        assertEquals(credential.getMechanism(), newCredential.getMechanism());
        assertEquals(credential.getUserName(), newCredential.getUserName());
        assertArrayEquals(credential.getPassword(), newCredential.getPassword());
        assertEquals(credential.getSource(), newCredential.getSource());
    }

    @Test
    @DisplayName("should throw IllegalArgumentException if username is not provided to a GSSAPI credential")
    void shouldThrowForMissingGSSAPIUsername() {
        assertThrows(IllegalArgumentException.class, () -> MongoCredential.createGSSAPICredential(null));
    }

    @Test
    @DisplayName("should make a copy of the password")
    void shouldMakeCopyOfPassword() {
        MongoCredential credential = MongoCredential.createPlainCredential("user", "source", "pwd".toCharArray());
        char[] password = credential.getPassword();
        password[0] = 's';

        assertArrayEquals(credential.getPassword(), credential.getPassword());
    }

    @Test
    @DisplayName("testObjectOverrides")
    void testObjectOverrides() {
        String userName = "user";
        String database = "test";
        String password = "pwd";
        String propertyKey = "keyOne";
        String propertyValue = "valueOne";

        MongoCredential credentialOne = MongoCredential.createScramSha256Credential(userName, database,
                password.toCharArray());
        MongoCredential credentialTwo = credentialOne.withMechanismProperty(propertyKey, propertyValue);

        assertEquals(MongoCredential.createScramSha256Credential(userName, database, password.toCharArray()),
                credentialOne);
        assertEquals(credentialOne.withMechanismProperty(propertyKey, propertyValue), credentialTwo);
        assertNotEquals(credentialOne, credentialTwo);

        assertEquals(
                MongoCredential.createScramSha256Credential(userName, database, password.toCharArray()).hashCode(),
                credentialOne.hashCode());
        assertNotEquals(credentialOne.hashCode(), credentialTwo.hashCode());

        assertFalse(credentialOne.toString().contains(password));
        assertTrue(credentialOne.toString().contains("password=<hidden>"));

        assertFalse(credentialTwo.toString().contains(propertyKey.toLowerCase()));
        assertFalse(credentialTwo.toString().contains(propertyValue));
        assertTrue(credentialTwo.toString().contains("mechanismProperties=<hidden>"));
    }

    @ParameterizedTest
    @MethodSource("equalsAndHashCodeData")
    @DisplayName("testEqualsAndHashCode")
    void testEqualsAndHashCode(final Supplier<MongoCredential> credentialSupplier) {
        assertEquals(credentialSupplier.get(), credentialSupplier.get());
        assertEquals(credentialSupplier.get().hashCode(), credentialSupplier.get().hashCode());
    }

    static Stream<Supplier<MongoCredential>> equalsAndHashCodeData() {
        return Stream.of(
                () -> MongoCredential.createCredential("user", "database", "pwd".toCharArray()),
                () -> MongoCredential.createCredential("user", "database", "pwd".toCharArray())
                        .withMechanismProperty("foo", "bar"),
                () -> MongoCredential.createPlainCredential("user", "$external", "pwd".toCharArray()),
                () -> MongoCredential.createPlainCredential("user", "$external", "pwd".toCharArray())
                        .withMechanismProperty("foo", "bar"),
                () -> MongoCredential.createScramSha1Credential("user", "$external", "pwd".toCharArray()),
                () -> MongoCredential.createScramSha1Credential("user", "$external", "pwd".toCharArray())
                        .withMechanismProperty("foo", "bar"),
                () -> MongoCredential.createGSSAPICredential("user"),
                () -> MongoCredential.createGSSAPICredential("user").withMechanismProperty("foo", "bar"),
                () -> MongoCredential.createMongoX509Credential("user"),
                () -> MongoCredential.createMongoX509Credential("user").withMechanismProperty("foo", "bar"),
                () -> MongoCredential.createMongoX509Credential(),
                () -> MongoCredential.createMongoX509Credential().withMechanismProperty("foo", "bar")
        );
    }
}
