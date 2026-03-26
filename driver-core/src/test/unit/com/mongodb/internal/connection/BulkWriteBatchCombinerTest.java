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

import com.mongodb.MongoBulkWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteInsert;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.bulk.WriteConcernError;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static com.mongodb.WriteConcern.ACKNOWLEDGED;
import static com.mongodb.WriteConcern.UNACKNOWLEDGED;
import static com.mongodb.internal.bulk.WriteRequest.Type.INSERT;
import static com.mongodb.internal.bulk.WriteRequest.Type.UPDATE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BulkWriteBatchCombinerTest {

    @Test
    void shouldGetUnacknowledgedResultForAnUnacknowledgedWrite() {
        BulkWriteBatchCombiner combiner = new BulkWriteBatchCombiner(new ServerAddress(), true, UNACKNOWLEDGED);
        combiner.addResult(BulkWriteResult.acknowledged(INSERT, 1, 0, Collections.emptyList(), Collections.emptyList()));

        BulkWriteResult result = combiner.getResult();

        assertEquals(BulkWriteResult.unacknowledged(), result);
    }

    @Test
    void shouldGetCorrectResultForAnInsert() {
        BulkWriteBatchCombiner combiner = new BulkWriteBatchCombiner(new ServerAddress(), true, ACKNOWLEDGED);
        combiner.addResult(BulkWriteResult.acknowledged(INSERT, 1, 0, Collections.emptyList(),
                Collections.singletonList(new BulkWriteInsert(6, new BsonString("id1")))));
        combiner.addResult(BulkWriteResult.acknowledged(INSERT, 1, 0, Collections.emptyList(),
                Collections.singletonList(new BulkWriteInsert(3, new BsonString("id2")))));

        BulkWriteResult result = combiner.getResult();

        assertEquals(BulkWriteResult.acknowledged(INSERT, 2, 0, Collections.emptyList(),
                Arrays.asList(new BulkWriteInsert(3, new BsonString("id2")), new BulkWriteInsert(6, new BsonString("id1")))), result);
    }

    @Test
    void shouldSortUpserts() {
        BulkWriteBatchCombiner combiner = new BulkWriteBatchCombiner(new ServerAddress(), true, ACKNOWLEDGED);
        combiner.addResult(BulkWriteResult.acknowledged(UPDATE, 1, 0,
                Collections.singletonList(new BulkWriteUpsert(6, new BsonString("id1"))), Collections.emptyList()));
        combiner.addResult(BulkWriteResult.acknowledged(UPDATE, 1, 0,
                Collections.singletonList(new BulkWriteUpsert(3, new BsonString("id2"))), Collections.emptyList()));

        BulkWriteResult result = combiner.getResult();

        assertEquals(BulkWriteResult.acknowledged(UPDATE, 2, 0,
                Arrays.asList(new BulkWriteUpsert(3, new BsonString("id2")),
                        new BulkWriteUpsert(6, new BsonString("id1"))), Collections.emptyList()), result);
    }

    @Test
    void shouldThrowExceptionOnWriteError() {
        BulkWriteBatchCombiner combiner = new BulkWriteBatchCombiner(new ServerAddress(), true, ACKNOWLEDGED);
        BulkWriteError error = new BulkWriteError(11000, "dup key", new BsonDocument(), 0);
        combiner.addWriteErrorResult(error, IndexMap.create().add(0, 0));

        MongoBulkWriteException e = assertThrows(MongoBulkWriteException.class, () -> combiner.getResult());

        assertEquals(new MongoBulkWriteException(BulkWriteResult.acknowledged(INSERT, 0, 0,
                Collections.emptyList(), Collections.emptyList()),
                Collections.singletonList(error), null, new ServerAddress(), new HashSet<>()), e);
    }

    @Test
    void shouldThrowLastWriteConcernError() {
        BulkWriteBatchCombiner combiner = new BulkWriteBatchCombiner(new ServerAddress(), true, ACKNOWLEDGED);
        combiner.addWriteConcernErrorResult(new WriteConcernError(65, "journalError", "journal error", new BsonDocument()));
        WriteConcernError writeConcernError = new WriteConcernError(75, "wtimeout", "wtimeout message", new BsonDocument());
        combiner.addWriteConcernErrorResult(writeConcernError);

        MongoBulkWriteException e = assertThrows(MongoBulkWriteException.class, () -> combiner.getResult());

        assertEquals(new MongoBulkWriteException(BulkWriteResult.acknowledged(INSERT, 0, 0,
                Collections.emptyList(), Collections.emptyList()),
                Collections.emptyList(), writeConcernError, new ServerAddress(), new HashSet<>()), e);
    }

    @Test
    void shouldNotStopRunIfNoErrors() {
        BulkWriteBatchCombiner combiner = new BulkWriteBatchCombiner(new ServerAddress(), true, ACKNOWLEDGED);
        combiner.addResult(BulkWriteResult.acknowledged(INSERT, 1, 0, Collections.emptyList(), Collections.emptyList()));

        assertFalse(combiner.shouldStopSendingMoreBatches());
    }

    @Test
    void shouldStopRunOnErrorIfOrdered() {
        BulkWriteBatchCombiner combiner = new BulkWriteBatchCombiner(new ServerAddress(), true, ACKNOWLEDGED);
        combiner.addWriteErrorResult(new BulkWriteError(11000, "dup key", new BsonDocument(), 0),
                IndexMap.create().add(0, 0));

        assertTrue(combiner.shouldStopSendingMoreBatches());
    }

    @Test
    void shouldNotStopRunOnErrorIfUnordered() {
        BulkWriteBatchCombiner combiner = new BulkWriteBatchCombiner(new ServerAddress(), false, ACKNOWLEDGED);
        combiner.addWriteErrorResult(new BulkWriteError(11000, "dup key", new BsonDocument(), 0),
                IndexMap.create().add(0, 0));

        assertFalse(combiner.shouldStopSendingMoreBatches());
    }

    @Test
    void shouldSortErrorsByFirstIndex() {
        BulkWriteBatchCombiner combiner = new BulkWriteBatchCombiner(new ServerAddress(), false, ACKNOWLEDGED);
        combiner.addErrorResult(
                Arrays.asList(
                        new BulkWriteError(11000, "dup key", new BsonDocument(), 1),
                        new BulkWriteError(45, "wc error", new BsonDocument(), 0)),
                null, IndexMap.create().add(0, 0).add(1, 1).add(2, 2));

        MongoBulkWriteException e = assertThrows(MongoBulkWriteException.class, () -> combiner.getResult());

        assertEquals(Arrays.asList(
                new BulkWriteError(45, "wc error", new BsonDocument(), 0),
                new BulkWriteError(11000, "dup key", new BsonDocument(), 1)),
                e.getWriteErrors());
    }
}
