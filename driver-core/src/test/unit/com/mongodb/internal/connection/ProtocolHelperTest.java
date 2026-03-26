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

import com.mongodb.MongoCommandException;
import com.mongodb.MongoExecutionTimeoutException;
import com.mongodb.MongoNodeIsRecoveringException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.MongoQueryException;
import com.mongodb.ServerAddress;
import com.mongodb.internal.TimeoutContext;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS;
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS_WITH_INFINITE_TIMEOUT;
import static com.mongodb.internal.connection.ProtocolHelper.getCommandFailureException;
import static com.mongodb.internal.connection.ProtocolHelper.getQueryFailureException;
import static com.mongodb.internal.connection.ProtocolHelper.isCommandOk;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProtocolHelperTest {

    @Test
    void isCommandOkShouldBeFalseIfOkFieldIsMissing() {
        assertFalse(isCommandOk(new BsonDocument()));
    }

    @Test
    void isCommandOkShouldBeFalseForNumbersThatAreZero() {
        assertFalse(isCommandOk(new BsonDocument("ok", new BsonInt32(0))));
        assertFalse(isCommandOk(new BsonDocument("ok", new BsonInt64(0))));
        assertFalse(isCommandOk(new BsonDocument("ok", new BsonDouble(0.0))));
    }

    @Test
    void isCommandOkShouldBeFalseForNumbersThatAreNotZero() {
        // Note: the original Spock test asserted !isCommandOk for non-zero numbers (10),
        // which seems intentional - isCommandOk only returns true for value 1
        assertFalse(isCommandOk(new BsonDocument("ok", new BsonInt32(10))));
        assertFalse(isCommandOk(new BsonDocument("ok", new BsonInt64(10))));
        assertFalse(isCommandOk(new BsonDocument("ok", new BsonDouble(10.0))));
    }

    @Test
    void isCommandOkShouldEqualTheBooleanValue() {
        assertTrue(isCommandOk(new BsonDocument("ok", BsonBoolean.TRUE)));
        assertFalse(isCommandOk(new BsonDocument("ok", BsonBoolean.FALSE)));
    }

    @Test
    void isCommandOkShouldBeFalseIfOkIsNotANumberOrBoolean() {
        assertFalse(isCommandOk(new BsonDocument("ok", new BsonNull())));
    }

    @Test
    void commandFailureExceptionShouldBeMongoExecutionTimeoutExceptionIfErrorCodeIs50() {
        assertInstanceOf(MongoExecutionTimeoutException.class,
                getCommandFailureException(new BsonDocument("ok", new BsonInt32(0)).append("code", new BsonInt32(50)),
                        new ServerAddress(), new TimeoutContext(TIMEOUT_SETTINGS)));
    }

    @Test
    void commandFailureExceptionShouldBeMongoOperationTimeoutExceptionIfErrorCodeIs50AndTimeoutMSIsSet() {
        assertInstanceOf(MongoOperationTimeoutException.class,
                getCommandFailureException(new BsonDocument("ok", new BsonInt32(0)).append("code", new BsonInt32(50)),
                        new ServerAddress(), new TimeoutContext(TIMEOUT_SETTINGS_WITH_INFINITE_TIMEOUT)));
    }

    @Test
    void queryFailureExceptionShouldBeMongoExecutionTimeoutExceptionIfErrorCodeIs50() {
        assertInstanceOf(MongoExecutionTimeoutException.class,
                getQueryFailureException(new BsonDocument("code", new BsonInt32(50)),
                        new ServerAddress(), new TimeoutContext(TIMEOUT_SETTINGS)));
    }

    @Test
    void queryFailureExceptionShouldBeMongoOperationTimeoutExceptionIfErrorCodeIs50() {
        Exception exception = getQueryFailureException(new BsonDocument("code", new BsonInt32(50)),
                new ServerAddress(), new TimeoutContext(TIMEOUT_SETTINGS_WITH_INFINITE_TIMEOUT));
        assertInstanceOf(MongoOperationTimeoutException.class, exception);
        assertInstanceOf(MongoExecutionTimeoutException.class, exception.getCause());
    }

    static Stream<BsonDocument> notPrimaryCommandExceptions() {
        return Stream.of(
                BsonDocument.parse("{ok: 0, errmsg: \"not master server\"}"),
                BsonDocument.parse("{ok: 0, code: 10107}"),
                BsonDocument.parse("{ok: 0, code: 13435}")
        );
    }

    @ParameterizedTest
    @MethodSource("notPrimaryCommandExceptions")
    void commandFailureExceptionsShouldHandleMongoNotPrimaryExceptionScenarios(BsonDocument exception) {
        assertInstanceOf(MongoNotPrimaryException.class,
                getCommandFailureException(exception, new ServerAddress(), new TimeoutContext(TIMEOUT_SETTINGS)));
    }

    static Stream<BsonDocument> notPrimaryQueryExceptions() {
        return Stream.of(
                BsonDocument.parse("{$err: \"not master server\"}"),
                BsonDocument.parse("{code: 10107}"),
                BsonDocument.parse("{code: 13435}")
        );
    }

    @ParameterizedTest
    @MethodSource("notPrimaryQueryExceptions")
    void queryFailureExceptionsShouldHandleMongoNotPrimaryExceptionScenarios(BsonDocument exception) {
        assertInstanceOf(MongoNotPrimaryException.class,
                getQueryFailureException(exception, new ServerAddress(), new TimeoutContext(TIMEOUT_SETTINGS)));
    }

    static Stream<BsonDocument> nodeIsRecoveringCommandExceptions() {
        return Stream.of(
                BsonDocument.parse("{ok: 0, errmsg: \"node is recovering now\"}"),
                BsonDocument.parse("{ok: 0, code: 11600}"),
                BsonDocument.parse("{ok: 0, code: 11602}"),
                BsonDocument.parse("{ok: 0, code: 13436}"),
                BsonDocument.parse("{ok: 0, code: 189}"),
                BsonDocument.parse("{ok: 0, code: 91}")
        );
    }

    @ParameterizedTest
    @MethodSource("nodeIsRecoveringCommandExceptions")
    void commandFailureExceptionsShouldHandleMongoNodeIsRecoveringExceptionScenarios(BsonDocument exception) {
        assertInstanceOf(MongoNodeIsRecoveringException.class,
                getCommandFailureException(exception, new ServerAddress(), new TimeoutContext(TIMEOUT_SETTINGS)));
    }

    static Stream<BsonDocument> nodeIsRecoveringQueryExceptions() {
        return Stream.of(
                BsonDocument.parse("{$err: \"node is recovering now\"}"),
                BsonDocument.parse("{code: 11600}"),
                BsonDocument.parse("{code: 11602}"),
                BsonDocument.parse("{code: 13436}"),
                BsonDocument.parse("{code: 189}"),
                BsonDocument.parse("{code: 91}")
        );
    }

    @ParameterizedTest
    @MethodSource("nodeIsRecoveringQueryExceptions")
    void queryFailureExceptionsShouldHandleMongoNodeIsRecoveringExceptionScenarios(BsonDocument exception) {
        assertInstanceOf(MongoNodeIsRecoveringException.class,
                getQueryFailureException(exception, new ServerAddress(), new TimeoutContext(TIMEOUT_SETTINGS)));
    }

    @Test
    void commandFailureExceptionShouldBeMongoCommandException() {
        assertInstanceOf(MongoCommandException.class,
                getCommandFailureException(
                        new BsonDocument("ok", new BsonInt32(0)).append("errmsg", new BsonString("some other problem")),
                        new ServerAddress(), new TimeoutContext(TIMEOUT_SETTINGS)));
    }

    @Test
    void queryFailureExceptionShouldBeMongoQueryException() {
        assertInstanceOf(MongoQueryException.class,
                getQueryFailureException(
                        new BsonDocument("$err", new BsonString("some other problem")),
                        new ServerAddress(), new TimeoutContext(TIMEOUT_SETTINGS)));
    }
}
