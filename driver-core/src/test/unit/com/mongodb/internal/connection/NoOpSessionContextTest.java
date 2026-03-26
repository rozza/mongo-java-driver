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

import com.mongodb.ReadConcern;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NoOpSessionContextTest {

    @Test
    void shouldBeANoOp() {
        NoOpSessionContext sessionContext = NoOpSessionContext.INSTANCE;

        assertFalse(sessionContext.hasSession());
        assertNull(sessionContext.getClusterTime());
        assertNull(sessionContext.getOperationTime());
        assertFalse(sessionContext.isCausallyConsistent());
        assertEquals(ReadConcern.DEFAULT, sessionContext.getReadConcern());

        // These should not throw
        sessionContext.advanceOperationTime(new BsonTimestamp(42, 1));
        sessionContext.advanceClusterTime(new BsonDocument());

        assertThrows(UnsupportedOperationException.class, () -> sessionContext.getSessionId());
        assertThrows(UnsupportedOperationException.class, () -> sessionContext.advanceTransactionNumber());
    }

    @Test
    void shouldProvideGivenReadConcernForReadConcernAwareNoOpSessionContext() {
        ReadConcernAwareNoOpSessionContext sessionContext = new ReadConcernAwareNoOpSessionContext(ReadConcern.MAJORITY);

        assertEquals(ReadConcern.MAJORITY, sessionContext.getReadConcern());
    }
}
