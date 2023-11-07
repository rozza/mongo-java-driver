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

import com.mongodb.MongoNamespace;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.client.model.changestream.ChangeStreamLevel;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.codecs.Decoder;
import org.bson.codecs.RawBsonDocumentCodec;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * An operation that executes an {@code $changeStream} aggregation.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class ChangeStreamOperation<T> implements AsyncReadOperation<AsyncBatchCursor<T>>, ReadOperation<BatchCursor<T>> {
    private static final RawBsonDocumentCodec RAW_BSON_DOCUMENT_CODEC = new RawBsonDocumentCodec();
    private final AggregateOperationImpl<RawBsonDocument> wrapped;
    private final FullDocument fullDocument;
    private final FullDocumentBeforeChange fullDocumentBeforeChange;
    private final Decoder<T> decoder;
    private final ChangeStreamLevel changeStreamLevel;

    private BsonDocument resumeAfter;
    private BsonDocument startAfter;
    private BsonTimestamp startAtOperationTime;
    private boolean showExpandedEvents;


    public ChangeStreamOperation(final TimeoutSettings timeoutSettings, final MongoNamespace namespace,
            final FullDocument fullDocument, final FullDocumentBeforeChange fullDocumentBeforeChange,
            final List<BsonDocument> pipeline, final Decoder<T> decoder) {
        this(timeoutSettings, namespace, fullDocument, fullDocumentBeforeChange, pipeline, decoder, ChangeStreamLevel.COLLECTION);
    }

    public ChangeStreamOperation(final TimeoutSettings timeoutSettings, final MongoNamespace namespace,
            final FullDocument fullDocument, final FullDocumentBeforeChange fullDocumentBeforeChange, final List<BsonDocument> pipeline,
            final Decoder<T> decoder, final ChangeStreamLevel changeStreamLevel) {
        this.wrapped = new AggregateOperationImpl<>(timeoutSettings, namespace, pipeline, RAW_BSON_DOCUMENT_CODEC,
                getAggregateTarget(), getPipelineCreator());
        this.fullDocument = notNull("fullDocument", fullDocument);
        this.fullDocumentBeforeChange = notNull("fullDocumentBeforeChange", fullDocumentBeforeChange);
        this.decoder = notNull("decoder", decoder);
        this.changeStreamLevel = notNull("changeStreamLevel", changeStreamLevel);
    }

    public MongoNamespace getNamespace() {
        return wrapped.getNamespace();
    }

    public Decoder<T> getDecoder() {
        return decoder;
    }

    public FullDocument getFullDocument() {
        return fullDocument;
    }

    public BsonDocument getResumeAfter() {
        return resumeAfter;
    }

    public ChangeStreamOperation<T> resumeAfter(final BsonDocument resumeAfter) {
        this.resumeAfter = resumeAfter;
        return this;
    }

    public BsonDocument getStartAfter() {
        return startAfter;
    }

    public ChangeStreamOperation<T> startAfter(final BsonDocument startAfter) {
        this.startAfter = startAfter;
        return this;
    }

    public List<BsonDocument> getPipeline() {
        return wrapped.getPipeline();
    }

    public Integer getBatchSize() {
        return wrapped.getBatchSize();
    }

    public ChangeStreamOperation<T> batchSize(@Nullable final Integer batchSize) {
        wrapped.batchSize(batchSize);
        return this;
    }

    public Collation getCollation() {
        return wrapped.getCollation();
    }

    public ChangeStreamOperation<T> collation(final Collation collation) {
        wrapped.collation(collation);
        return this;
    }

    public ChangeStreamOperation<T> startAtOperationTime(final BsonTimestamp startAtOperationTime) {
        this.startAtOperationTime = startAtOperationTime;
        return this;
    }

    public BsonTimestamp getStartAtOperationTime() {
        return startAtOperationTime;
    }

    public ChangeStreamOperation<T> retryReads(final boolean retryReads) {
        wrapped.retryReads(retryReads);
        return this;
    }

    public boolean getRetryReads() {
        return wrapped.getRetryReads();
    }

    @Nullable
    public BsonValue getComment() {
        return wrapped.getComment();
    }

    public ChangeStreamOperation<T> comment(final BsonValue comment) {
        wrapped.comment(comment);
        return this;
    }

    public boolean getShowExpandedEvents() {
        return this.showExpandedEvents;
    }

    public ChangeStreamOperation<T> showExpandedEvents(final boolean showExpandedEvents) {
        this.showExpandedEvents = showExpandedEvents;
        return this;
    }

    @Override
    public TimeoutSettings getTimeoutSettings() {
        return wrapped.getTimeoutSettings();
    }

    @Override
    public BatchCursor<T> execute(final ReadBinding binding) {
        CommandBatchCursor<RawBsonDocument> cursor = (CommandBatchCursor<RawBsonDocument>) wrapped.execute(binding);
            return new ChangeStreamBatchCursor<>(ChangeStreamOperation.this, cursor, binding,
                    setChangeStreamOptions(cursor.getPostBatchResumeToken(), cursor.getOperationTime(),
                            cursor.getMaxWireVersion(), cursor.isFirstBatchEmpty()), cursor.getMaxWireVersion());
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        wrapped.executeAsync(binding, (result, t) -> {
            if (t != null) {
                callback.onResult(null, t);
            } else {
                AsyncCommandBatchCursor<RawBsonDocument> cursor = (AsyncCommandBatchCursor<RawBsonDocument>) assertNotNull(result);
                callback.onResult(new AsyncChangeStreamBatchCursor<>(ChangeStreamOperation.this, cursor, binding,
                        setChangeStreamOptions(cursor.getPostBatchResumeToken(), cursor.getOperationTime(),
                                cursor.getMaxWireVersion(), cursor.isFirstBatchEmpty()), cursor.getMaxWireVersion()), null);
            }
        });
    }

    @Nullable
    private BsonDocument setChangeStreamOptions(@Nullable final BsonDocument postBatchResumeToken,
            @Nullable final BsonTimestamp operationTime, final int maxWireVersion, final boolean firstBatchEmpty) {
        BsonDocument resumeToken = null;
        if (startAfter != null) {
            resumeToken = startAfter;
        } else if (resumeAfter != null) {
            resumeToken = resumeAfter;
        } else if (startAtOperationTime == null && postBatchResumeToken == null && firstBatchEmpty && maxWireVersion >= 7) {
            startAtOperationTime = operationTime;
        }
        return resumeToken;
    }

    public void setChangeStreamOptionsForResume(@Nullable final BsonDocument resumeToken, final int maxWireVersion) {
        startAfter = null;
        if (resumeToken != null) {
            startAtOperationTime = null;
            resumeAfter = resumeToken;
        } else if (startAtOperationTime != null && maxWireVersion >= 7) {
            resumeAfter = null;
        } else {
            resumeAfter = null;
            startAtOperationTime = null;
        }
    }

    // Leave as anonymous class so as not to confuse CustomMatchers#compare
    private AggregateOperationImpl.AggregateTarget getAggregateTarget() {
        return () -> changeStreamLevel == ChangeStreamLevel.COLLECTION
                ? new BsonString(getNamespace().getCollectionName()) : new BsonInt32(1);
    }

    // Leave as anonymous class so as not to confuse CustomMatchers#compare
    private AggregateOperationImpl.PipelineCreator getPipelineCreator() {
        return () -> {
            List<BsonDocument> changeStreamPipeline = new ArrayList<>();
            BsonDocument changeStream = new BsonDocument();
            if (fullDocument != FullDocument.DEFAULT) {
                changeStream.append("fullDocument", new BsonString(fullDocument.getValue()));
            }
            if (fullDocumentBeforeChange != FullDocumentBeforeChange.DEFAULT) {
                changeStream.append("fullDocumentBeforeChange", new BsonString(fullDocumentBeforeChange.getValue()));
            }

            if (changeStreamLevel == ChangeStreamLevel.CLIENT) {
                changeStream.append("allChangesForCluster", BsonBoolean.TRUE);
            }

            if (showExpandedEvents) {
                changeStream.append("showExpandedEvents", BsonBoolean.TRUE);
            }

            if (resumeAfter != null) {
                changeStream.append("resumeAfter", resumeAfter);
            }
            if (startAfter != null) {
                changeStream.append("startAfter", startAfter);
            }
            if (startAtOperationTime != null) {
                changeStream.append("startAtOperationTime", startAtOperationTime);
            }

            changeStreamPipeline.add(new BsonDocument("$changeStream", changeStream));
            changeStreamPipeline.addAll(getPipeline());
            return new BsonArray(changeStreamPipeline);
        };
    }
}
