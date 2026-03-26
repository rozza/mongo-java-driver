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

import com.mongodb.MongoNamespace;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ChangeStreamDocumentTest {

    @Test
    void shouldInitializeCorrectly() {
        RawBsonDocument resumeToken = RawBsonDocument.parse("{token: true}");
        BsonDocument namespaceDocument = BsonDocument.parse("{db: \"databaseName\", coll: \"collectionName\"}");
        MongoNamespace namespace = new MongoNamespace("databaseName.collectionName");
        NamespaceType namespaceType = NamespaceType.COLLECTION;
        BsonDocument destinationNamespaceDocument = BsonDocument.parse("{db: \"databaseName2\", coll: \"collectionName2\"}");
        MongoNamespace destinationNamespace = new MongoNamespace("databaseName2.collectionName2");
        BsonDocument fullDocument = BsonDocument.parse("{key: \"value for fullDocument\"}");
        BsonDocument fullDocumentBeforeChange = BsonDocument.parse("{key: \"value for fullDocumentBeforeChange\"}");
        BsonDocument documentKey = BsonDocument.parse("{_id : 1}");
        BsonTimestamp clusterTime = new BsonTimestamp(1234, 2);
        OperationType operationType = OperationType.UPDATE;
        UpdateDescription updateDesc = new UpdateDescription(Arrays.asList("a", "b"), BsonDocument.parse("{c: 1}"), null);
        BsonInt64 txnNumber = new BsonInt64(1);
        BsonDocument lsid = BsonDocument.parse("{id: 1, uid: 1}");
        BsonDateTime wallTime = new BsonDateTime(42);
        SplitEvent splitEvent = new SplitEvent(1, 2);
        BsonDocument extraElements = new BsonDocument("extra", BsonBoolean.TRUE);

        ChangeStreamDocument<BsonDocument> changeStreamDocument = new ChangeStreamDocument<>(
                operationType.getValue(), resumeToken,
                namespaceDocument, namespaceType.getValue(),
                destinationNamespaceDocument, fullDocument,
                fullDocumentBeforeChange, documentKey,
                clusterTime, updateDesc, txnNumber,
                lsid, wallTime, splitEvent, extraElements);

        assertEquals(resumeToken, changeStreamDocument.getResumeToken());
        assertEquals(fullDocument, changeStreamDocument.getFullDocument());
        assertEquals(fullDocumentBeforeChange, changeStreamDocument.getFullDocumentBeforeChange());
        assertEquals(documentKey, changeStreamDocument.getDocumentKey());
        assertEquals(clusterTime, changeStreamDocument.getClusterTime());
        assertEquals(namespace, changeStreamDocument.getNamespace());
        assertEquals(namespaceDocument, changeStreamDocument.getNamespaceDocument());
        assertEquals(namespaceType, changeStreamDocument.getNamespaceType());
        assertEquals(namespaceType.getValue(), changeStreamDocument.getNamespaceTypeString());
        assertEquals(destinationNamespace, changeStreamDocument.getDestinationNamespace());
        assertEquals(destinationNamespaceDocument, changeStreamDocument.getDestinationNamespaceDocument());
        assertEquals(operationType.getValue(), changeStreamDocument.getOperationTypeString());
        assertEquals(operationType, changeStreamDocument.getOperationType());
        assertEquals(updateDesc, changeStreamDocument.getUpdateDescription());
        assertEquals(namespace.getDatabaseName(), changeStreamDocument.getDatabaseName());
        assertEquals(txnNumber, changeStreamDocument.getTxnNumber());
        assertEquals(lsid, changeStreamDocument.getLsid());
        assertEquals(wallTime, changeStreamDocument.getWallTime());
        assertEquals(splitEvent, changeStreamDocument.getSplitEvent());
        assertEquals(extraElements, changeStreamDocument.getExtraElements());
    }

    @Test
    void shouldHandleNullNamespaceCorrectly() {
        RawBsonDocument resumeToken = RawBsonDocument.parse("{token: true}");
        BsonDocument fullDocument = BsonDocument.parse("{key: \"value for fullDocument\"}");
        BsonDocument fullDocumentBeforeChange = BsonDocument.parse("{key: \"value for fullDocumentBeforeChange\"}");
        BsonDocument documentKey = BsonDocument.parse("{_id : 1}");
        BsonTimestamp clusterTime = new BsonTimestamp(1234, 2);
        OperationType operationType = OperationType.DROP_DATABASE;
        UpdateDescription updateDesc = new UpdateDescription(Arrays.asList("a", "b"), BsonDocument.parse("{c: 1}"),
                Collections.emptyList());
        BsonDateTime wallTime = new BsonDateTime(42);
        SplitEvent splitEvent = new SplitEvent(1, 2);
        BsonDocument extraElements = new BsonDocument("extra", BsonBoolean.TRUE);

        ChangeStreamDocument<BsonDocument> doc = new ChangeStreamDocument<>(
                operationType.getValue(), resumeToken,
                (BsonDocument) null, null, (BsonDocument) null, fullDocument, fullDocumentBeforeChange,
                documentKey, clusterTime, updateDesc,
                null, null, wallTime, splitEvent, extraElements);

        assertNull(doc.getDatabaseName());
        assertNull(doc.getNamespace());
        assertNull(doc.getNamespaceType());
        assertNull(doc.getNamespaceTypeString());
        assertNull(doc.getNamespaceDocument());
        assertNull(doc.getDestinationNamespace());
        assertNull(doc.getDestinationNamespaceDocument());
    }

    @Test
    void shouldReturnNullOnMissingBsonDocumentElements() {
        RawBsonDocument resumeToken = RawBsonDocument.parse("{token: true}");
        BsonDocument namespaceDocument = BsonDocument.parse("{db: \"databaseName\"}");
        BsonDocument namespaceDocumentEmpty = new BsonDocument();
        BsonDocument fullDocument = BsonDocument.parse("{key: \"value for fullDocument\"}");
        BsonDocument fullDocumentBeforeChange = BsonDocument.parse("{key: \"value for fullDocumentBeforeChange\"}");
        BsonDocument documentKey = BsonDocument.parse("{_id : 1}");
        BsonTimestamp clusterTime = new BsonTimestamp(1234, 2);
        UpdateDescription updateDesc = new UpdateDescription(Arrays.asList("a", "b"), BsonDocument.parse("{c: 1}"),
                Collections.singletonList(new TruncatedArray("d", 1)));
        BsonDateTime wallTime = new BsonDateTime(42);
        SplitEvent splitEvent = new SplitEvent(1, 2);
        BsonDocument extraElements = new BsonDocument("extra", BsonBoolean.TRUE);

        ChangeStreamDocument<BsonDocument> changeStreamDocument = new ChangeStreamDocument<>(
                null, resumeToken, namespaceDocument, null,
                (BsonDocument) null, fullDocument, fullDocumentBeforeChange, documentKey, clusterTime,
                updateDesc, null, null, wallTime, splitEvent, extraElements);

        ChangeStreamDocument<BsonDocument> changeStreamDocumentEmptyNamespace = new ChangeStreamDocument<>(
                null, resumeToken, namespaceDocumentEmpty, null,
                (BsonDocument) null, fullDocument, fullDocumentBeforeChange,
                documentKey, clusterTime, updateDesc,
                null, null, wallTime, splitEvent, extraElements);

        assertNull(changeStreamDocument.getNamespace());
        assertNull(changeStreamDocument.getNamespaceType());
        assertNull(changeStreamDocument.getNamespaceTypeString());
        assertEquals("databaseName", changeStreamDocument.getDatabaseName());
        assertNull(changeStreamDocument.getOperationTypeString());
        assertNull(changeStreamDocument.getOperationType());

        assertNull(changeStreamDocumentEmptyNamespace.getNamespace());
        assertNull(changeStreamDocumentEmptyNamespace.getDatabaseName());
    }
}
