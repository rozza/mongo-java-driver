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
import com.mongodb.WriteConcern;
import com.mongodb.client.model.Collation;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.ClientSideOperationTimeout;
import com.mongodb.internal.ClientSideOperationTimeoutFactory;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.internal.validator.CollectibleDocumentFieldNameValidator;
import com.mongodb.internal.validator.MappedFieldNameValidator;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import com.mongodb.lang.Nullable;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.FieldNameValidator;
import org.bson.codecs.Decoder;
import org.bson.conversions.Bson;

import java.util.HashMap;
import java.util.Map;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotNull;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.internal.operation.DocumentHelper.putIfTrue;
import static com.mongodb.internal.operation.OperationHelper.validateCollation;
import static com.mongodb.internal.operation.OperationHelper.validateHint;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsAtLeastVersionThreeDotTwo;

/**
 * An operation that atomically finds and replaces a single document.
 *
 * @param <T> the operations result type.
 * @since 3.0
 * @mongodb.driver.manual reference/command/findAndModify/ findAndModify
 */
public class FindAndReplaceOperation<T> extends BaseFindAndModifyOperation<T> {
    private final BsonDocument replacement;
    private BsonDocument filter;
    private BsonDocument projection;
    private BsonDocument sort;
    private boolean returnOriginal = true;
    private boolean upsert;
    private Boolean bypassDocumentValidation;
    private Collation collation;
    private Bson hint;
    private String hintString;

    /**
     * Construct a new instance.
     *
     * @param clientSideOperationTimeoutFactory the client side operation timeout factory
     * @param namespace   the database and collection namespace for the operation.
     * @param decoder     the decoder for the result documents.
     * @param replacement the document that will replace the found document.
     */
    public FindAndReplaceOperation(final ClientSideOperationTimeoutFactory clientSideOperationTimeoutFactory,
                                   final MongoNamespace namespace, final Decoder<T> decoder, final BsonDocument replacement) {
        this(clientSideOperationTimeoutFactory, namespace, WriteConcern.ACKNOWLEDGED, false, decoder, replacement);
    }

    /**
     * Construct a new instance.
     *
     * @param clientSideOperationTimeoutFactory the client side operation timeout factory
     * @param namespace   the database and collection namespace for the operation.
     * @param writeConcern the writeConcern for the operation
     * @param decoder     the decoder for the result documents.
     * @param replacement the document that will replace the found document.
     * @since 3.2
     */
    public FindAndReplaceOperation(final ClientSideOperationTimeoutFactory clientSideOperationTimeoutFactory,
                                   final MongoNamespace namespace, final WriteConcern writeConcern, final Decoder<T> decoder,
                                   final BsonDocument replacement) {
        this(clientSideOperationTimeoutFactory, namespace, writeConcern, false, decoder, replacement);
    }

    /**
     * Construct a new instance.
     *
     * @param clientSideOperationTimeoutFactory the client side operation timeout factory
     * @param namespace   the database and collection namespace for the operation.
     * @param writeConcern the writeConcern for the operation
     * @param retryWrites  if writes should be retried if they fail due to a network error.
     * @param decoder     the decoder for the result documents.
     * @param replacement the document that will replace the found document.
     * @since 3.6
     */
    public FindAndReplaceOperation(final ClientSideOperationTimeoutFactory clientSideOperationTimeoutFactory,
                                   final MongoNamespace namespace, final WriteConcern writeConcern, final boolean retryWrites,
                                   final Decoder<T> decoder,
                                   final BsonDocument replacement) {
        super(clientSideOperationTimeoutFactory, namespace, writeConcern, retryWrites, decoder);
        this.replacement = notNull("replacement", replacement);
    }

    /**
     * Gets the document which will replace the document matching the query filter.
     *
     * @return the replacement document
     */
    public BsonDocument getReplacement() {
        return replacement;
    }

    /**
     * Gets the query filter.
     *
     * @return the query filter
     * @mongodb.driver.manual reference/method/db.collection.find/ Filter
     */
    public BsonDocument getFilter() {
        return filter;
    }

    /**
     * Sets the query filter to apply to the query.
     *
     * @param filter the query filter, which may be null.
     * @return this
     * @mongodb.driver.manual reference/method/db.collection.find/ Filter
     */
    public FindAndReplaceOperation<T> filter(final BsonDocument filter) {
        this.filter = filter;
        return this;
    }

    /**
     * Gets a document describing the fields to return for all matching documents.
     *
     * @return the project document, which may be null
     * @mongodb.driver.manual reference/method/db.collection.find/ Projection
     */
    public BsonDocument getProjection() {
        return projection;
    }

    /**
     * Sets a document describing the fields to return for all matching documents.
     *
     * @param projection the project document, which may be null.
     * @return this
     * @mongodb.driver.manual reference/method/db.collection.find/ Projection
     */
    public FindAndReplaceOperation<T> projection(final BsonDocument projection) {
        this.projection = projection;
        return this;
    }

    /**
     * Gets the sort criteria to apply to the query. The default is null, which means that the documents will be returned in an undefined
     * order.
     *
     * @return a document describing the sort criteria
     * @mongodb.driver.manual reference/method/cursor.sort/ Sort
     */
    public BsonDocument getSort() {
        return sort;
    }

    /**
     * Sets the sort criteria to apply to the query.
     *
     * @param sort the sort criteria, which may be null.
     * @return this
     * @mongodb.driver.manual reference/method/cursor.sort/ Sort
     */
    public FindAndReplaceOperation<T> sort(final BsonDocument sort) {
        this.sort = sort;
        return this;
    }

    /**
     * When false, returns the replaced document rather than the original. The default is false.
     *
     * @return true if the original document should be returned
     */
    public boolean isReturnOriginal() {
        return returnOriginal;
    }

    /**
     * Set to false to return the replaced document rather than the original.
     *
     * @param returnOriginal set to false to return the replaced document rather than the original
     * @return this
     */
    public FindAndReplaceOperation<T> returnOriginal(final boolean returnOriginal) {
        this.returnOriginal = returnOriginal;
        return this;
    }

    /**
     * Returns true if a new document should be inserted if there are no matches to the query filter.  The default is false.
     *
     * @return true if a new document should be inserted if there are no matches to the query filter
     */
    public boolean isUpsert() {
        return upsert;
    }

    /**
     * Set to true if a new document should be inserted if there are no matches to the query filter.
     *
     * @param upsert true if a new document should be inserted if there are no matches to the query filter
     * @return this
     */
    public FindAndReplaceOperation<T> upsert(final boolean upsert) {
        this.upsert = upsert;
        return this;
    }

    /**
     * Gets the bypass document level validation flag
     *
     * @return the bypass document level validation flag
     * @since 3.2
     */
    public Boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    /**
     * Sets the bypass document level validation flag.
     *
     * @param bypassDocumentValidation If true, allows the write to opt-out of document level validation.
     * @return this
     * @since 3.2
     * @mongodb.driver.manual reference/command/aggregate/ Aggregation
     * @mongodb.server.release 3.2
     */
    public FindAndReplaceOperation<T> bypassDocumentValidation(final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    /**
     * Returns the collation options
     *
     * @return the collation options
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    public Collation getCollation() {
        return collation;
    }

    /**
     * Returns the hint for which index to use. The default is not to set a hint.
     *
     * @return the hint
     * @since 4.1
     */
    @Nullable
    public Bson getHint() {
        return hint;
    }

    /**
     * Sets the hint for which index to use. A null value means no hint is set.
     *
     * @param hint the hint
     * @return this
     * @since 4.1
     */
    public FindAndReplaceOperation<T> hint(@Nullable final Bson hint) {
        this.hint = hint;
        return this;
    }

    /**
     * Gets the hint string to apply.
     *
     * @return the hint string, which should be the name of an existing index
     * @since 4.1
     */
    @Nullable
    public String getHintString() {
        return hintString;
    }

    /**
     * Sets the hint to apply.
     *
     * @param hint the name of the index which should be used for the operation
     * @return this
     * @since 4.1
     */
    public FindAndReplaceOperation<T> hintString(@Nullable final String hint) {
        this.hintString = hint;
        return this;
    }

    /**
     * Sets the collation options
     *
     * <p>A null value represents the server default.</p>
     * @param collation the collation options to use
     * @return this
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    public FindAndReplaceOperation<T> collation(final Collation collation) {
        this.collation = collation;
        return this;
    }

    @Override
    protected String getDatabaseName() {
        return getNamespace().getDatabaseName();
    }

    @Override
    protected CommandCreator getCommandCreator(final SessionContext sessionContext) {
        return new CommandCreator() {
            @Override
            public BsonDocument create(final ClientSideOperationTimeout clientSideOperationTimeout,
                                       final ServerDescription serverDescription, final ConnectionDescription connectionDescription) {
                return createCommand(clientSideOperationTimeout, sessionContext, serverDescription, connectionDescription);
            }
        };
    }

    private BsonDocument createCommand(final ClientSideOperationTimeout clientSideOperationTimeout, final SessionContext sessionContext,
                                       final ServerDescription serverDescription, final ConnectionDescription connectionDescription) {
        validateCollation(connectionDescription, collation);

        BsonDocument commandDocument = new BsonDocument("findAndModify", new BsonString(getNamespace().getCollectionName()));
        putIfNotNull(commandDocument, "query", getFilter());
        putIfNotNull(commandDocument, "fields", getProjection());
        putIfNotNull(commandDocument, "sort", getSort());
        commandDocument.put("new", new BsonBoolean(!isReturnOriginal()));
        putIfTrue(commandDocument, "upsert", isUpsert());
        putIfNotZero(commandDocument, "maxTimeMS", clientSideOperationTimeout.getMaxTimeMS());
        commandDocument.put("update", getReplacement());
        if (bypassDocumentValidation != null && serverIsAtLeastVersionThreeDotTwo(connectionDescription)) {
            commandDocument.put("bypassDocumentValidation", BsonBoolean.valueOf(bypassDocumentValidation));
        }
        addWriteConcernToCommand(connectionDescription, commandDocument, sessionContext);
        if (collation != null) {
            commandDocument.put("collation", collation.asDocument());
        }
        if (hint != null || hintString != null) {
            validateHint(connectionDescription, getWriteConcern());
            if (hint != null) {
                commandDocument.put("hint", hint.toBsonDocument(BsonDocument.class, null));
            } else {
                commandDocument.put("hint", new BsonString(hintString));
            }
        }
        addTxnNumberToCommand(serverDescription, connectionDescription, commandDocument, sessionContext);
        return commandDocument;
    }

    @Override
    protected FieldNameValidator getFieldNameValidator() {
        Map<String, FieldNameValidator> map = new HashMap<String, FieldNameValidator>();
        map.put("update", new CollectibleDocumentFieldNameValidator());
        return new MappedFieldNameValidator(new NoOpFieldNameValidator(), map);
    }
}
