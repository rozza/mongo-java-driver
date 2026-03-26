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

package com.mongodb.client.model.changestream;

import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWriter;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonReader;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ChangeStreamDocumentCodecTest {

    private final CodecRegistry codecRegistry = fromProviders(new DocumentCodecProvider(), new ValueCodecProvider());

    @Test
    void shouldRoundTripChangeStreamDocumentSuccessfully() {
        List<ChangeStreamDocument<Document>> documents = Arrays.asList(
                new ChangeStreamDocument<>(OperationType.INSERT.getValue(),
                        BsonDocument.parse("{token: true}"),
                        BsonDocument.parse("{db: \"engineering\", coll: \"users\"}"),
                        NamespaceType.COLLECTION.getValue(),
                        null,
                        Document.parse("{_id: 1, userName: \"alice123\", name: \"Alice\"}"),
                        null,
                        BsonDocument.parse("{userName: \"alice123\", _id: 1}"),
                        new BsonTimestamp(1234, 2),
                        null, null, null, null,
                        new SplitEvent(3, 4),
                        null),
                new ChangeStreamDocument<>(OperationType.UPDATE.getValue(),
                        BsonDocument.parse("{token: true}"),
                        BsonDocument.parse("{db: \"engineering\", coll: \"users\"}"),
                        NamespaceType.COLLECTION.getValue(),
                        null,
                        null,
                        null,
                        BsonDocument.parse("{_id: 1}"),
                        new BsonTimestamp(1234, 2),
                        new UpdateDescription(Collections.singletonList("phoneNumber"),
                                BsonDocument.parse("{email: \"alice@10gen.com\"}"), null),
                        null, null, null, null, null),
                new ChangeStreamDocument<>(OperationType.UPDATE.getValue(),
                        BsonDocument.parse("{token: true}"),
                        BsonDocument.parse("{db: \"engineering\", coll: \"users\"}"),
                        NamespaceType.COLLECTION.getValue(),
                        null,
                        Document.parse("{_id: 1, userName: \"alice123\", name: \"Alice\"}"),
                        Document.parse("{_id: 1, userName: \"alice1234\", name: \"Alice\"}"),
                        BsonDocument.parse("{_id: 1}"),
                        new BsonTimestamp(1234, 2),
                        new UpdateDescription(Collections.singletonList("phoneNumber"),
                                BsonDocument.parse("{email: \"alice@10gen.com\"}"),
                                Collections.singletonList(new TruncatedArray("education", 2))),
                        null, null, null, null, null),
                new ChangeStreamDocument<>(OperationType.REPLACE.getValue(),
                        BsonDocument.parse("{token: true}"),
                        BsonDocument.parse("{db: \"engineering\", coll: \"users\"}"),
                        NamespaceType.COLLECTION.getValue(),
                        null,
                        Document.parse("{_id: 1, userName: \"alice123\", name: \"Alice\"}"),
                        Document.parse("{_id: 1, userName: \"alice1234\", name: \"Alice\"}"),
                        BsonDocument.parse("{_id: 1}"),
                        new BsonTimestamp(1234, 2),
                        null, null, null, null, null, null),
                new ChangeStreamDocument<>(OperationType.DELETE.getValue(),
                        BsonDocument.parse("{token: true}"),
                        BsonDocument.parse("{db: \"engineering\", coll: \"users\"}"),
                        NamespaceType.COLLECTION.getValue(),
                        null,
                        null,
                        Document.parse("{_id: 1, userName: \"alice123\", name: \"Alice\"}"),
                        BsonDocument.parse("{_id: 1}"),
                        new BsonTimestamp(1234, 2),
                        null, null, null, null, null, null),
                new ChangeStreamDocument<>(OperationType.DROP.getValue(),
                        BsonDocument.parse("{token: true}"),
                        BsonDocument.parse("{db: \"engineering\", coll: \"users\"}"),
                        NamespaceType.COLLECTION.getValue(),
                        null,
                        null,
                        null,
                        null,
                        new BsonTimestamp(1234, 2),
                        null, null, null, null, null, null),
                new ChangeStreamDocument<>(OperationType.RENAME.getValue(),
                        BsonDocument.parse("{token: true}"),
                        BsonDocument.parse("{db: \"engineering\", coll: \"users\"}"),
                        NamespaceType.COLLECTION.getValue(),
                        BsonDocument.parse("{db: \"engineering\", coll: \"people\"}"),
                        null,
                        null,
                        null,
                        new BsonTimestamp(1234, 2),
                        null, null, null, null, null, null),
                new ChangeStreamDocument<>(OperationType.DROP_DATABASE.getValue(),
                        BsonDocument.parse("{token: true}"),
                        BsonDocument.parse("{db: \"engineering\"}"),
                        null,
                        null,
                        null,
                        null,
                        null,
                        new BsonTimestamp(1234, 2),
                        null, null, null, null, null, null),
                new ChangeStreamDocument<>(OperationType.INVALIDATE.getValue(),
                        BsonDocument.parse("{token: true}"),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        new BsonTimestamp(1234, 2),
                        null, null, null, null, null, null),
                new ChangeStreamDocument<>(OperationType.INSERT.getValue(),
                        BsonDocument.parse("{token: true}"),
                        BsonDocument.parse("{db: \"engineering\", coll: \"users\"}"),
                        NamespaceType.COLLECTION.getValue(),
                        null,
                        Document.parse("{_id: 1, userName: \"alice123\", name: \"Alice\"}"),
                        null,
                        BsonDocument.parse("{userName: \"alice123\", _id: 1}"),
                        new BsonTimestamp(1234, 2),
                        null,
                        new BsonInt64(1),
                        BsonDocument.parse("{id: 1, uid: 2}"),
                        new BsonDateTime(42), null,
                        new BsonDocument("extra", BsonBoolean.TRUE).append("value", new BsonInt32(1)))
        );

        List<String> jsons = Arrays.asList(
                "{ _id: { token : true }, operationType: 'insert', "
                        + "clusterTime: { \"$timestamp\" : { \"t\" : 1234, \"i\" : 2 } }, "
                        + "ns: { db: 'engineering', coll: 'users' }, nsType: 'collection', "
                        + "documentKey: { userName: 'alice123', _id: 1 }, "
                        + "fullDocument: { _id: 1, userName: 'alice123', name: 'Alice' }, "
                        + "splitEvent: { fragment: 3, of: 4 } }",
                "{ _id: { token : true }, operationType: 'update', "
                        + "clusterTime: { \"$timestamp\" : { \"t\" : 1234, \"i\" : 2 } }, "
                        + "ns: { db: 'engineering', coll: 'users' }, nsType: 'collection', "
                        + "documentKey: { _id: 1 }, "
                        + "updateDescription: { updatedFields: { email: 'alice@10gen.com' }, "
                        + "removedFields: ['phoneNumber'], \"truncatedArrays\": [] } }",
                "{ _id: { token : true }, operationType: 'update', "
                        + "clusterTime: { \"$timestamp\" : { \"t\" : 1234, \"i\" : 2 } }, "
                        + "ns: { db: 'engineering', coll: 'users' }, nsType: 'collection', "
                        + "documentKey: { _id: 1 }, "
                        + "updateDescription: { updatedFields: { email: 'alice@10gen.com' }, "
                        + "removedFields: ['phoneNumber'], \"truncatedArrays\": [ { \"field\": \"education\", \"newSize\": 2 } ] }, "
                        + "fullDocument: { _id: 1, name: 'Alice', userName: 'alice123' }, "
                        + "fullDocumentBeforeChange: { _id: 1, name: 'Alice', userName: 'alice1234' } }",
                "{ _id: { token : true }, operationType: 'replace', "
                        + "clusterTime: { \"$timestamp\" : { \"t\" : 1234, \"i\" : 2 } }, "
                        + "ns: { db: 'engineering', coll: 'users' }, nsType: 'collection', "
                        + "documentKey: { _id: 1 }, "
                        + "fullDocument: { _id: 1, userName: 'alice123', name: 'Alice' }, "
                        + "fullDocumentBeforeChange: { _id: 1, name: 'Alice', userName: 'alice1234' } }",
                "{ _id: { token : true }, operationType: 'delete', "
                        + "clusterTime: { \"$timestamp\" : { \"t\" : 1234, \"i\" : 2 } }, "
                        + "ns: { db: 'engineering', coll: 'users' }, nsType: 'collection', "
                        + "documentKey: { _id: 1 }, "
                        + "fullDocumentBeforeChange: { _id: 1, name: 'Alice', userName: 'alice123' } }",
                "{ _id: { token : true }, operationType: 'drop', "
                        + "clusterTime: { \"$timestamp\" : { \"t\" : 1234, \"i\" : 2 } }, "
                        + "ns: { db: 'engineering', coll: 'users' }, nsType: 'collection' }",
                "{ _id: { token : true }, operationType: 'rename', "
                        + "clusterTime: { \"$timestamp\" : { \"t\" : 1234, \"i\" : 2 } }, "
                        + "ns: { db: 'engineering', coll: 'users' }, nsType: 'collection', "
                        + "to: { db: 'engineering', coll: 'people' } }",
                "{ _id: { token : true }, operationType: 'dropDatabase', "
                        + "clusterTime: { \"$timestamp\" : { \"t\" : 1234, \"i\" : 2 } }, "
                        + "ns: { db: 'engineering' } }",
                "{ _id: { token : true }, operationType: 'invalidate', "
                        + "clusterTime: { \"$timestamp\" : { \"t\" : 1234, \"i\" : 2 } } }",
                "{ _id: { token : true }, operationType: 'insert', "
                        + "clusterTime: { \"$timestamp\" : { \"t\" : 1234, \"i\" : 2 } }, "
                        + "ns: { db: 'engineering', coll: 'users' }, nsType: 'collection', "
                        + "documentKey: { userName: 'alice123', _id: 1 }, "
                        + "fullDocument: { _id: 1, userName: 'alice123', name: 'Alice' }, "
                        + "txnNumber: NumberLong('1'), lsid: { id: 1, uid: 2 }, "
                        + "wallTime: {$date: 42}, extra: true, value: 1 }"
        );

        for (int i = 0; i < documents.size(); i++) {
            ChangeStreamDocument<Document> changeStreamDocument = documents.get(i);
            ChangeStreamDocumentCodec<Document> codec = new ChangeStreamDocumentCodec<>(Document.class, codecRegistry);

            BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());
            codec.encode(writer, changeStreamDocument, EncoderContext.builder().build());
            assertEquals(BsonDocument.parse(jsons.get(i)), writer.getDocument());

            BsonReader bsonReader = new BsonDocumentReader(writer.getDocument());
            @SuppressWarnings("unchecked")
            ChangeStreamDocument<Document> actual = codec.decode(bsonReader, DecoderContext.builder().build());
            assertEquals(changeStreamDocument, actual);
        }
    }
}
