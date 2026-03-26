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

import com.mongodb.ReadConcern;
import com.mongodb.internal.session.SessionContext;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.junit.jupiter.api.Test;

import static com.mongodb.internal.connection.ReadConcernHelper.getReadConcernDocument;
import static com.mongodb.internal.operation.ServerVersionHelper.UNKNOWN_WIRE_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ReadConcernHelperTest {

    @Test
    void shouldThrowIllegalArgumentExceptionIfSessionContextIsNull() {
        assertThrows(IllegalArgumentException.class, () -> getReadConcernDocument(null, UNKNOWN_WIRE_VERSION));
    }

    @Test
    void shouldAddAfterClusterTimeToMajorityReadConcernWhenSessionIsCausallyConsistent() {
        BsonTimestamp operationTime = new BsonTimestamp(42, 1);
        SessionContext sessionContext = mock(SessionContext.class);
        when(sessionContext.isCausallyConsistent()).thenReturn(true);
        when(sessionContext.getOperationTime()).thenReturn(operationTime);
        when(sessionContext.getReadConcern()).thenReturn(ReadConcern.MAJORITY);

        assertEquals(
                new BsonDocument("level", new BsonString("majority"))
                        .append("afterClusterTime", operationTime),
                getReadConcernDocument(sessionContext, UNKNOWN_WIRE_VERSION));
    }

    @Test
    void shouldAddAfterClusterTimeToDefaultReadConcernWhenSessionIsCausallyConsistent() {
        BsonTimestamp operationTime = new BsonTimestamp(42, 1);
        SessionContext sessionContext = mock(SessionContext.class);
        when(sessionContext.isCausallyConsistent()).thenReturn(true);
        when(sessionContext.getOperationTime()).thenReturn(operationTime);
        when(sessionContext.getReadConcern()).thenReturn(ReadConcern.DEFAULT);

        assertEquals(
                new BsonDocument("afterClusterTime", operationTime),
                getReadConcernDocument(sessionContext, UNKNOWN_WIRE_VERSION));
    }

    @Test
    void shouldNotAddAfterClusterTimeToReadConcernWhenSessionIsNotCausallyConsistent() {
        SessionContext sessionContext = mock(SessionContext.class);
        when(sessionContext.isCausallyConsistent()).thenReturn(false);
        when(sessionContext.getReadConcern()).thenReturn(ReadConcern.MAJORITY);

        assertEquals(
                new BsonDocument("level", new BsonString("majority")),
                getReadConcernDocument(sessionContext, UNKNOWN_WIRE_VERSION));
    }

    @Test
    void shouldNotAddAfterClusterTimeToReadConcernWhenOperationTimeIsNull() {
        SessionContext sessionContext = mock(SessionContext.class);
        when(sessionContext.isCausallyConsistent()).thenReturn(true);
        when(sessionContext.getOperationTime()).thenReturn(null);
        when(sessionContext.getReadConcern()).thenReturn(ReadConcern.MAJORITY);

        assertEquals(
                new BsonDocument("level", new BsonString("majority")),
                getReadConcernDocument(sessionContext, UNKNOWN_WIRE_VERSION));
    }
}
