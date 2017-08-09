/*
 * Copyright 2017 MongoDB, Inc.
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

package com.mongodb.operation;

import com.mongodb.Function;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoQueryException;
import com.mongodb.MongoServerException;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.binding.ReadBinding;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static java.util.Collections.singletonList;

final class ChangeStreamBatchCursor<T> implements BatchCursor<T> {
    private static final BsonDocumentCodec BSON_DOCUMENT_CODEC = new BsonDocumentCodec();
    private final ReadBinding binding;
    private final ChangeStreamOperation<T> changeStreamOperation;

    private volatile BsonDocument resumeToken;
    private volatile QueryBatchCursor<RawBsonDocument> wrapped;

    ChangeStreamBatchCursor(final ChangeStreamOperation<T> changeStreamOperation, final ReadBinding binding,
                            final QueryBatchCursor<RawBsonDocument> wrapped) {
        this.changeStreamOperation = changeStreamOperation;
        this.resumeToken = changeStreamOperation.getResumeToken();
        this.binding = binding;
        this.wrapped = wrapped;
    }

    QueryBatchCursor<RawBsonDocument> getWrapped() {
        return wrapped;
    }

    @Override
    public boolean hasNext() {
        return resumeAbleOperation(new Function<QueryBatchCursor<RawBsonDocument>, Boolean>() {
            @Override
            public Boolean apply(final QueryBatchCursor<RawBsonDocument> queryBatchCursor) {
                return queryBatchCursor.hasNext();
            }
        });
    }

    @Override
    public List<T> next() {
        return resumeAbleOperation(new Function<QueryBatchCursor<RawBsonDocument>, List<T>>() {
            @Override
            public List<T> apply(final QueryBatchCursor<RawBsonDocument> queryBatchCursor) {
                return convertResults(queryBatchCursor.next());
            }
        });
    }

    @Override
    public List<T> tryNext() {
        return resumeAbleOperation(new Function<QueryBatchCursor<RawBsonDocument>, List<T>>() {
            @Override
            public List<T> apply(final QueryBatchCursor<RawBsonDocument> queryBatchCursor) {
                return convertResults(queryBatchCursor.tryNext());
            }
        });
    }

    @Override
    public void close() {
        wrapped.close();
    }

    @Override
    public void setBatchSize(final int batchSize) {
        wrapped.setBatchSize(batchSize);
    }

    @Override
    public int getBatchSize() {
        return wrapped.getBatchSize();
    }

    @Override
    public ServerCursor getServerCursor() {
        return wrapped.getServerCursor();
    }

    @Override
    public ServerAddress getServerAddress() {
        return wrapped.getServerAddress();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    private List<T> convertResults(final List<RawBsonDocument> rawDocuments) {
        List<T> results = null;
        if (rawDocuments != null) {
            results = new ArrayList<T>();
            for (RawBsonDocument rawDocument : rawDocuments) {
                resumeToken = rawDocument.getDocument("_id");
                results.add(rawDocument.decode(changeStreamOperation.getCodec()));
            }
        }
        return results;
    }

    <R> R resumeAbleOperation(final Function<QueryBatchCursor<RawBsonDocument>, R> function) {
        if (changeStreamOperation.isResumable()) {
            try {
                return function.apply(wrapped);
            } catch (Exception e) {
                if (e instanceof MongoServerException && !(e instanceof MongoNotPrimaryException || e instanceof MongoQueryException)) {
                    throw (MongoServerException) e;
                }

                killPreviousCursor(wrapped.getServerCursor());
                wrapped = ((ChangeStreamBatchCursor<T>) new ChangeStreamOperation<T>(changeStreamOperation, resumeToken)
                        .execute(binding)).getWrapped();
                return function.apply(wrapped);
            }
        } else {
            return function.apply(wrapped);
        }
    }

    private void killPreviousCursor(final ServerCursor serverCursor) {
        if (serverCursor != null) {
            MongoNamespace namespace = changeStreamOperation.getNamespace();
            BsonDocument command = new BsonDocument("killCursors", new BsonString(namespace.getCollectionName()))
                    .append("cursors", new BsonArray(singletonList(new BsonInt64(serverCursor.getId()))));
            try {
                executeWrappedCommandProtocol(binding, namespace.getDatabaseName(), command, BSON_DOCUMENT_CODEC);
            } catch (Exception e) {
                // Ignore any exceptions killing old cursors
            }
        }
    }

}
