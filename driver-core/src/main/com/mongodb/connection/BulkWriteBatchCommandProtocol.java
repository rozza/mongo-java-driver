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

package com.mongodb.connection;

import com.mongodb.MongoNamespace;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.internal.client.model.BulkWriteBatch;
import org.bson.BsonDocument;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.ProtocolHelper.getMessageSettings;

class BulkWriteBatchCommandProtocol implements CommandProtocol<BulkWriteResult> {

    public static final Logger LOGGER = Loggers.getLogger("protocol.command");
    private final MongoNamespace namespace;
    private final BulkWriteBatch initialBatch;
    private SessionContext sessionContext;

    BulkWriteBatchCommandProtocol(final String database, final BulkWriteBatch initialBatch) {
        notNull("database", database);
        this.namespace = new MongoNamespace(notNull("database", database), MongoNamespace.COMMAND_COLLECTION_NAME);
        this.initialBatch = notNull("initialBatch", initialBatch);
    }

    @Override
    public BulkWriteResult execute(final InternalConnection connection) {
        BulkWriteBatch batch = initialBatch;
        while (batch.shouldProcessBatch()) {
            SplittablePayloadCommandMessage commandMessage = new SplittablePayloadCommandMessage(namespace, batch.getCommand(),
                    batch.getPayload(), getMessageSettings(connection.getDescription()));
            BsonDocument result = connection.sendAndReceive(commandMessage, batch.getDecoder(), sessionContext);
            batch.addResult(result);
            batch = batch.getNextBatch();
        }
        return batch.getResult();
    }

    @Override
    public void executeAsync(final InternalConnection connection, final SingleResultCallback<BulkWriteResult> callback) {
        executeAsync(connection, initialBatch, callback);
    }

    private void executeAsync(final InternalConnection connection, final BulkWriteBatch batch,
                              final SingleResultCallback<BulkWriteResult> callback) {
        SplittablePayloadCommandMessage commandMessage = new SplittablePayloadCommandMessage(namespace, batch.getCommand(),
                batch.getPayload(), getMessageSettings(connection.getDescription()));
        connection.sendAndReceiveAsync(commandMessage, batch.getDecoder(), sessionContext, new SingleResultCallback<BsonDocument>() {
            @Override
            public void onResult(final BsonDocument result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    batch.addResult(result);
                    BulkWriteBatch nextBatch = batch.getNextBatch();
                    if (nextBatch.shouldProcessBatch()) {
                        executeAsync(connection, nextBatch, callback);
                    } else {
                        if (batch.hasErrors()) {
                            callback.onResult(null, batch.getError());
                        } else {
                            callback.onResult(batch.getResult(), null);
                        }
                    }
                }
            }
        });
    }

    @Override
    public BulkWriteBatchCommandProtocol sessionContext(final SessionContext sessionContext) {
        this.sessionContext = sessionContext;
        return this;
    }
}
