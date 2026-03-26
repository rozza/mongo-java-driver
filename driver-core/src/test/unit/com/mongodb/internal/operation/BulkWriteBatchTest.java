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

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteInsert;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.client.model.Collation;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ConnectionId;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.ServerType;
import com.mongodb.internal.IgnorableRequestContext;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.connection.SplittablePayload;
import com.mongodb.internal.bulk.DeleteRequest;
import com.mongodb.internal.bulk.InsertRequest;
import com.mongodb.internal.bulk.UpdateRequest;
import com.mongodb.internal.bulk.WriteRequest;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.connection.ReadConcernAwareNoOpSessionContext;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.mongodb.connection.ServerConnectionState.CONNECTED;
import static com.mongodb.internal.bulk.WriteRequest.Type.REPLACE;
import static com.mongodb.internal.bulk.WriteRequest.Type.UPDATE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BulkWriteBatchTest {

    private static final TimeoutContext TIMEOUT_CONTEXT = new TimeoutContext(new TimeoutSettings(0, 0, 0, 0L, 0));
    private final MongoNamespace namespace = new MongoNamespace("db.coll");
    private final ServerDescription serverDescription = ServerDescription.builder().address(new ServerAddress()).state(CONNECTED)
            .logicalSessionTimeoutMinutes(30).build();
    private final ConnectionDescription connectionDescription = new ConnectionDescription(
            new ConnectionId(new ServerId(new ClusterId(), serverDescription.getAddress())), 6,
            ServerType.REPLICA_SET_PRIMARY, 1000, 16000, 48000, Collections.emptyList());
    private final OperationContext operationContext = new OperationContext(IgnorableRequestContext.INSTANCE,
            new ReadConcernAwareNoOpSessionContext(ReadConcern.DEFAULT), TIMEOUT_CONTEXT, null);

    @Test
    void shouldSplitPayloadsByTypeWhenOrdered() {
        BulkWriteBatch bulkWriteBatch = BulkWriteBatch.createBulkWriteBatch(namespace, connectionDescription, true,
                WriteConcern.ACKNOWLEDGED, null, false, getWriteRequests(), operationContext, null, null);
        SplittablePayload payload = bulkWriteBatch.getPayload();
        payload.setPosition(payload.size());

        assertEquals("documents", payload.getPayloadName());
        assertEquals(Collections.singletonList(getWriteRequestsAsDocuments().get(0)), payload.getPayload());
        assertEquals(toBsonDocument("{ \"insert\" : \"coll\", \"ordered\" : true }"), bulkWriteBatch.getCommand());
        assertTrue(bulkWriteBatch.hasAnotherBatch());

        bulkWriteBatch = bulkWriteBatch.getNextBatch();
        payload = bulkWriteBatch.getPayload();
        payload.setPosition(payload.size());

        assertEquals("updates", payload.getPayloadName());
        assertEquals(getWriteRequestsAsDocuments().subList(1, 3), payload.getPayload());
        assertEquals(toBsonDocument("{ \"update\" : \"coll\", \"ordered\" : true }"), bulkWriteBatch.getCommand());
        assertTrue(bulkWriteBatch.hasAnotherBatch());

        bulkWriteBatch = bulkWriteBatch.getNextBatch();
        payload = bulkWriteBatch.getPayload();
        payload.setPosition(payload.size());

        assertEquals("documents", payload.getPayloadName());
        assertEquals(getWriteRequestsAsDocuments().subList(3, 5), payload.getPayload());
        assertEquals(toBsonDocument("{ \"insert\" : \"coll\", \"ordered\" : true }"), bulkWriteBatch.getCommand());
        assertTrue(bulkWriteBatch.hasAnotherBatch());

        bulkWriteBatch = bulkWriteBatch.getNextBatch();
        payload = bulkWriteBatch.getPayload();
        payload.setPosition(payload.size());

        assertEquals("updates", payload.getPayloadName());
        assertEquals(Collections.singletonList(getWriteRequestsAsDocuments().get(5)), payload.getPayload());
        assertEquals(toBsonDocument("{ \"update\" : \"coll\", \"ordered\" : true }"), bulkWriteBatch.getCommand());
        assertTrue(bulkWriteBatch.hasAnotherBatch());

        bulkWriteBatch = bulkWriteBatch.getNextBatch();
        payload = bulkWriteBatch.getPayload();
        payload.setPosition(payload.size());

        assertEquals("deletes", payload.getPayloadName());
        assertEquals(getWriteRequestsAsDocuments().subList(6, 8), payload.getPayload());
        assertEquals(toBsonDocument("{ \"delete\" : \"coll\", \"ordered\" : true }"), bulkWriteBatch.getCommand());
        assertTrue(bulkWriteBatch.hasAnotherBatch());

        bulkWriteBatch = bulkWriteBatch.getNextBatch();
        payload = bulkWriteBatch.getPayload();
        payload.setPosition(payload.size());

        assertEquals("documents", payload.getPayloadName());
        assertEquals(Collections.singletonList(getWriteRequestsAsDocuments().get(8)), payload.getPayload());
        assertEquals(toBsonDocument("{ \"insert\" : \"coll\", \"ordered\" : true }"), bulkWriteBatch.getCommand());
        assertTrue(bulkWriteBatch.hasAnotherBatch());

        bulkWriteBatch = bulkWriteBatch.getNextBatch();
        payload = bulkWriteBatch.getPayload();
        payload.setPosition(payload.size());

        assertEquals("deletes", payload.getPayloadName());
        assertEquals(Collections.singletonList(getWriteRequestsAsDocuments().get(9)), payload.getPayload());
        assertEquals(toBsonDocument("{ \"delete\" : \"coll\", \"ordered\" : true }"), bulkWriteBatch.getCommand());
        assertFalse(bulkWriteBatch.hasAnotherBatch());
    }

    @Test
    void shouldGroupPayloadsByTypeWhenUnordered() {
        BulkWriteBatch bulkWriteBatch = BulkWriteBatch.createBulkWriteBatch(namespace, connectionDescription, false,
                WriteConcern.MAJORITY, true, false, getWriteRequests(), operationContext, null, null);
        SplittablePayload payload = bulkWriteBatch.getPayload();
        payload.setPosition(payload.size());

        assertEquals("documents", payload.getPayloadName());
        assertEquals(Arrays.asList(getWriteRequestsAsDocuments().get(0), getWriteRequestsAsDocuments().get(3),
                getWriteRequestsAsDocuments().get(4), getWriteRequestsAsDocuments().get(8)), payload.getPayload());
        assertTrue(bulkWriteBatch.hasAnotherBatch());
        assertEquals(toBsonDocument("{\"insert\": \"coll\", \"ordered\": false, "
                + "\"writeConcern\": {\"w\" : \"majority\"}, \"bypassDocumentValidation\" : true }"), bulkWriteBatch.getCommand());

        bulkWriteBatch = bulkWriteBatch.getNextBatch();
        payload = bulkWriteBatch.getPayload();
        payload.setPosition(payload.size());

        assertEquals("updates", payload.getPayloadName());
        assertEquals(Arrays.asList(getWriteRequestsAsDocuments().get(1), getWriteRequestsAsDocuments().get(2)),
                payload.getPayload());
        assertTrue(bulkWriteBatch.hasAnotherBatch());

        bulkWriteBatch = bulkWriteBatch.getNextBatch();
        payload = bulkWriteBatch.getPayload();
        payload.setPosition(payload.size());

        assertEquals("updates", payload.getPayloadName());
        assertEquals(Collections.singletonList(getWriteRequestsAsDocuments().get(5)), payload.getPayload());
        assertTrue(bulkWriteBatch.hasAnotherBatch());

        bulkWriteBatch = bulkWriteBatch.getNextBatch();
        payload = bulkWriteBatch.getPayload();
        payload.setPosition(payload.size());

        assertEquals("deletes", payload.getPayloadName());
        assertEquals(Arrays.asList(getWriteRequestsAsDocuments().get(6), getWriteRequestsAsDocuments().get(7),
                getWriteRequestsAsDocuments().get(9)), payload.getPayload());
        assertFalse(bulkWriteBatch.hasAnotherBatch());
    }

    @Test
    void shouldNotRetryWhenAtLeastOneWriteIsNotRetryable() {
        BulkWriteBatch bulkWriteBatch = BulkWriteBatch.createBulkWriteBatch(namespace, connectionDescription, false,
                WriteConcern.ACKNOWLEDGED, null, true,
                Arrays.asList(new DeleteRequest(new BsonDocument()).multi(true), new InsertRequest(new BsonDocument())),
                operationContext, null, null);
        assertFalse(bulkWriteBatch.getRetryWrites());
    }

    @Test
    void shouldHandleOperationResponses() {
        BulkWriteBatch bulkWriteBatch = BulkWriteBatch.createBulkWriteBatch(namespace, connectionDescription, true,
                WriteConcern.ACKNOWLEDGED, null, false, getWriteRequests().subList(1, 2), operationContext, null, null);
        BsonDocument response = toBsonDocument("{ok: 1, n: 1, upserted: [{_id: 2, index: 0}]}");

        bulkWriteBatch.addResult(response);

        assertFalse(bulkWriteBatch.hasErrors());
        assertEquals(BulkWriteResult.acknowledged(0, 0, 0, 0,
                Collections.singletonList(new BulkWriteUpsert(0, new BsonInt32(2))),
                Collections.emptyList()), bulkWriteBatch.getResult());
        assertTrue(bulkWriteBatch.shouldProcessBatch());
    }

    @Test
    void shouldHandleWriteConcernErrorResponses() {
        BulkWriteBatch bulkWriteBatch = BulkWriteBatch.createBulkWriteBatch(namespace, connectionDescription, true,
                WriteConcern.ACKNOWLEDGED, null, false, getWriteRequests().subList(0, 1), operationContext, null, null);
        BsonDocument response = toBsonDocument(
                "{n: 1, writeConcernError: {code: 75, errmsg: \"wtimeout\", errInfo: {wtimeout: \"0\"}}}");

        bulkWriteBatch.addResult(response);

        assertTrue(bulkWriteBatch.hasErrors());
        assertTrue(bulkWriteBatch.getError().getWriteErrors().isEmpty());
        assertNotNull(bulkWriteBatch.getError().getWriteConcernError());
        assertTrue(bulkWriteBatch.shouldProcessBatch());
    }

    @Test
    void shouldHandleWriteErrorResponses() {
        BulkWriteBatch bulkWriteBatch = BulkWriteBatch.createBulkWriteBatch(namespace, connectionDescription, true,
                WriteConcern.ACKNOWLEDGED, null, false, getWriteRequests().subList(0, 1), operationContext, null, null);
        BsonDocument response = toBsonDocument("{\"ok\": 0, \"n\": 1, \"code\": 65, \"errmsg\": \"bulk op errors\","
                + "\"writeErrors\": [{ \"index\" : 0, \"code\" : 100, \"errmsg\": \"some error\"}] }");

        bulkWriteBatch.addResult(response);

        assertTrue(bulkWriteBatch.hasErrors());
        assertEquals(1, bulkWriteBatch.getError().getWriteErrors().size());
        assertFalse(bulkWriteBatch.shouldProcessBatch());
    }

    @Test
    void shouldMapAllInsertedIds() {
        BulkWriteBatch bulkWriteBatch = BulkWriteBatch.createBulkWriteBatch(namespace, connectionDescription, false,
                WriteConcern.ACKNOWLEDGED, null, false,
                Arrays.asList(
                        new InsertRequest(toBsonDocument("{_id: 0}")),
                        new InsertRequest(toBsonDocument("{_id: 1}")),
                        new InsertRequest(toBsonDocument("{_id: 2}"))
                ),
                operationContext, null, null);
        SplittablePayload payload = bulkWriteBatch.getPayload();
        payload.setPosition(1);
        payload.getInsertedIds().put(0, new BsonInt32(0));
        bulkWriteBatch.addResult(BsonDocument.parse("{\"n\": 1, \"ok\": 1.0}"));

        assertEquals(Collections.singletonList(new BulkWriteInsert(0, new BsonInt32(0))),
                bulkWriteBatch.getResult().getInserts());

        bulkWriteBatch = bulkWriteBatch.getNextBatch();
        payload = bulkWriteBatch.getPayload();
        payload.setPosition(1);
        payload.getInsertedIds().put(1, new BsonInt32(1));
        bulkWriteBatch.addResult(BsonDocument.parse("{\"n\": 1, \"ok\": 1.0}"));

        assertEquals(Arrays.asList(new BulkWriteInsert(0, new BsonInt32(0)), new BulkWriteInsert(1, new BsonInt32(1))),
                bulkWriteBatch.getResult().getInserts());

        bulkWriteBatch = bulkWriteBatch.getNextBatch();
        payload = bulkWriteBatch.getPayload();
        payload.setPosition(1);
        payload.getInsertedIds().put(2, new BsonInt32(2));
        bulkWriteBatch.addResult(BsonDocument.parse("{\"n\": 1, \"ok\": 1.0}"));

        assertEquals(Arrays.asList(new BulkWriteInsert(0, new BsonInt32(0)),
                        new BulkWriteInsert(1, new BsonInt32(1)),
                        new BulkWriteInsert(2, new BsonInt32(2))),
                bulkWriteBatch.getResult().getInserts());
    }

    @Test
    void shouldNotMapInsertedIdWithWriteError() {
        BulkWriteBatch bulkWriteBatch = BulkWriteBatch.createBulkWriteBatch(namespace, connectionDescription, false,
                WriteConcern.ACKNOWLEDGED, null, false,
                Arrays.asList(
                        new InsertRequest(toBsonDocument("{_id: 0}")),
                        new InsertRequest(toBsonDocument("{_id: 1}")),
                        new InsertRequest(toBsonDocument("{_id: 2}"))
                ),
                operationContext, null, null);
        SplittablePayload payload = bulkWriteBatch.getPayload();
        payload.setPosition(3);
        payload.getInsertedIds().put(0, new BsonInt32(0));
        payload.getInsertedIds().put(1, new BsonInt32(1));
        payload.getInsertedIds().put(2, new BsonInt32(2));

        bulkWriteBatch.addResult(toBsonDocument("{\"ok\": 1, \"n\": 2,"
                + "\"writeErrors\": [{ \"index\" : 1, \"code\" : 11000, \"errmsg\": \"duplicate key error\"}] }"));
        MongoBulkWriteException ex = assertThrows(MongoBulkWriteException.class, bulkWriteBatch::getResult);
        assertEquals(Arrays.asList(new BulkWriteInsert(0, new BsonInt32(0)), new BulkWriteInsert(2, new BsonInt32(2))),
                ex.getWriteResult().getInserts());
    }

    private static List<WriteRequest> getWriteRequests() {
        return Arrays.asList(
                new InsertRequest(toBsonDocument("{_id: 1, x: 1}")),
                new UpdateRequest(toBsonDocument("{ _id: 2}"), toBsonDocument("{$set: {x : 2}}"), UPDATE).upsert(true),
                new UpdateRequest(toBsonDocument("{ _id: 3}"), toBsonDocument("{$set: {x : 3}}"), UPDATE),
                new InsertRequest(toBsonDocument("{_id: 4, x: 4}")),
                new InsertRequest(toBsonDocument("{_id: 5, x: 5}")),
                new UpdateRequest(toBsonDocument("{ _id: 6}"), toBsonDocument("{_id: 6, x: 6}"), REPLACE)
                        .collation(Collation.builder().locale("en").build()),
                new DeleteRequest(toBsonDocument("{_id: 7}")),
                new DeleteRequest(toBsonDocument("{_id: 8}")),
                new InsertRequest(toBsonDocument("{_id: 9, x: 9}")),
                new DeleteRequest(toBsonDocument("{_id: 10}")).collation(Collation.builder().locale("de").build())
        );
    }

    private static List<BsonDocument> getWriteRequestsAsDocuments() {
        return Arrays.asList(
                toBsonDocument("{_id: 1, x: 1}"),
                toBsonDocument("{\"q\": { \"_id\" : 2}, \"u\": { \"$set\": {\"x\": 2}}, \"multi\": true, \"upsert\": true }"),
                toBsonDocument("{\"q\": { \"_id\" : 3}, \"u\": { \"$set\": {\"x\": 3}}, \"multi\": true}"),
                toBsonDocument("{\"_id\": 4, \"x\": 4}"),
                toBsonDocument("{\"_id\": 5, \"x\": 5}"),
                toBsonDocument("{\"q\": { \"_id\" : 6 }, \"u\": { \"_id\": 6, \"x\": 6 }, \"collation\": { \"locale\": \"en\" }}"),
                toBsonDocument("{\"q\": { \"_id\" : 7 }, \"limit\": 0 }"),
                toBsonDocument("{\"q\": { \"_id\" : 8 }, \"limit\": 0 }"),
                toBsonDocument("{\"_id\": 9, \"x\": 9}"),
                toBsonDocument("{\"q\": { \"_id\" : 10 }, \"limit\": 0, \"collation\" : { \"locale\" : \"de\" }}")
        );
    }

    private static BsonDocument toBsonDocument(final String json) {
        return BsonDocument.parse(json);
    }
}
