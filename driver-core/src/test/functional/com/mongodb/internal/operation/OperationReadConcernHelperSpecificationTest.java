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

package com.mongodb.internal.operation;

import com.mongodb.ReadConcern;
import com.mongodb.internal.session.SessionContext;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.junit.jupiter.api.Test;

import static com.mongodb.internal.operation.OperationReadConcernHelper.appendReadConcernToCommand;
import static com.mongodb.internal.operation.ServerVersionHelper.UNKNOWN_WIRE_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OperationReadConcernHelperSpecificationTest {

    @Test
    void shouldThrowIllegalArgumentExceptionIfCommandDocumentIsNull() {
        SessionContext sessionContext = mock(SessionContext.class);
        assertThrows(IllegalArgumentException.class, () ->
                appendReadConcernToCommand(sessionContext, UNKNOWN_WIRE_VERSION, null));
    }

    @Test
    void shouldThrowIllegalArgumentExceptionIfSessionContextIsNull() {
        assertThrows(IllegalArgumentException.class, () ->
                appendReadConcernToCommand(null, UNKNOWN_WIRE_VERSION, new BsonDocument()));
    }

    @Test
    void shouldAddAfterClusterTimeToMajorityReadConcernWhenSessionIsCausallyConsistent() {
        BsonTimestamp operationTime = new BsonTimestamp(42, 1);
        SessionContext sessionContext = mock(SessionContext.class);
        when(sessionContext.isCausallyConsistent()).thenReturn(true);
        when(sessionContext.getOperationTime()).thenReturn(operationTime);
        when(sessionContext.getReadConcern()).thenReturn(ReadConcern.MAJORITY);

        BsonDocument commandDocument = new BsonDocument();
        appendReadConcernToCommand(sessionContext, UNKNOWN_WIRE_VERSION, commandDocument);

        assertEquals(new BsonDocument("readConcern",
                new BsonDocument("level", new BsonString("majority"))
                        .append("afterClusterTime", operationTime)),
                commandDocument);
    }

    @Test
    void shouldAddAfterClusterTimeToDefaultReadConcernWhenSessionIsCausallyConsistent() {
        BsonTimestamp operationTime = new BsonTimestamp(42, 1);
        SessionContext sessionContext = mock(SessionContext.class);
        when(sessionContext.isCausallyConsistent()).thenReturn(true);
        when(sessionContext.getOperationTime()).thenReturn(operationTime);
        when(sessionContext.getReadConcern()).thenReturn(ReadConcern.DEFAULT);

        BsonDocument commandDocument = new BsonDocument();
        appendReadConcernToCommand(sessionContext, UNKNOWN_WIRE_VERSION, commandDocument);

        assertEquals(new BsonDocument("readConcern",
                new BsonDocument("afterClusterTime", operationTime)),
                commandDocument);
    }

    @Test
    void shouldNotAddAfterClusterTimeToReadConcernWhenSessionIsNotCausallyConsistent() {
        SessionContext sessionContext = mock(SessionContext.class);
        when(sessionContext.isCausallyConsistent()).thenReturn(false);
        when(sessionContext.getReadConcern()).thenReturn(ReadConcern.MAJORITY);

        BsonDocument commandDocument = new BsonDocument();
        appendReadConcernToCommand(sessionContext, UNKNOWN_WIRE_VERSION, commandDocument);

        assertEquals(new BsonDocument("readConcern",
                new BsonDocument("level", new BsonString("majority"))),
                commandDocument);
    }

    @Test
    void shouldNotAddDefaultReadConcernToCommandDocument() {
        SessionContext sessionContext = mock(SessionContext.class);
        when(sessionContext.isCausallyConsistent()).thenReturn(false);
        when(sessionContext.getReadConcern()).thenReturn(ReadConcern.DEFAULT);

        BsonDocument commandDocument = new BsonDocument();
        appendReadConcernToCommand(sessionContext, UNKNOWN_WIRE_VERSION, commandDocument);

        assertEquals(new BsonDocument(), commandDocument);
    }

    @Test
    void shouldNotAddAfterClusterTimeToReadConcernWhenOperationTimeIsNull() {
        SessionContext sessionContext = mock(SessionContext.class);
        when(sessionContext.isCausallyConsistent()).thenReturn(true);
        when(sessionContext.getOperationTime()).thenReturn(null);
        when(sessionContext.getReadConcern()).thenReturn(ReadConcern.MAJORITY);

        BsonDocument commandDocument = new BsonDocument();
        appendReadConcernToCommand(sessionContext, UNKNOWN_WIRE_VERSION, commandDocument);

        assertEquals(new BsonDocument("readConcern",
                new BsonDocument("level", new BsonString("majority"))),
                commandDocument);
    }
}
