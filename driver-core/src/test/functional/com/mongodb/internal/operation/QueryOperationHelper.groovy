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

package com.mongodb.internal.operation

import com.mongodb.MongoCommandException
import com.mongodb.MongoCursorNotFoundException
import com.mongodb.MongoNamespace
import com.mongodb.MongoQueryException
import com.mongodb.ReadPreference
import com.mongodb.ServerCursor
import com.mongodb.async.FutureResultCallback
import com.mongodb.internal.IgnorableRequestContext
import com.mongodb.internal.binding.StaticBindingContext
import com.mongodb.internal.connection.AsyncConnection
import com.mongodb.internal.connection.Connection
import com.mongodb.internal.connection.NoOpSessionContext
import com.mongodb.internal.connection.OperationContext
import com.mongodb.internal.validator.NoOpFieldNameValidator
import org.bson.BsonDocument
import org.bson.BsonInt64
import org.bson.BsonString
import org.bson.codecs.BsonDocumentCodec

import static com.mongodb.ClusterFixture.getServerApi

class QueryOperationHelper {

    static BsonDocument getKeyPattern(BsonDocument explainPlan) {
        BsonDocument winningPlan = explainPlan.getDocument('queryPlanner').getDocument('winningPlan')
        if (winningPlan.containsKey('queryPlan')) {
            BsonDocument queryPlan = winningPlan.getDocument('queryPlan')
            if (queryPlan.containsKey('inputStage')) {
                return queryPlan.getDocument('inputStage').getDocument('keyPattern')
            }
        } else if (winningPlan.containsKey('inputStage')) {
            return winningPlan.getDocument('inputStage').getDocument('keyPattern')
        } else if (winningPlan.containsKey('shards')) {
            // recurse on shards[0] to get its query plan
            return getKeyPattern(new BsonDocument('queryPlanner', winningPlan.getArray('shards')[0].asDocument()))
        }
    }

    static void makeAdditionalGetMoreCall(MongoNamespace namespace, ServerCursor serverCursor, Connection connection) {
        makeAdditionalGetMoreCallHandleError(serverCursor) {
            connection.command(namespace.databaseName,
                    new BsonDocument('getMore', new BsonInt64(serverCursor.getId()))
                            .append('collection', new BsonString(namespace.getCollectionName())),
                    new NoOpFieldNameValidator(), ReadPreference.primary(),
                    new BsonDocumentCodec(),
                    new StaticBindingContext(new NoOpSessionContext(), getServerApi(), IgnorableRequestContext.INSTANCE,
                            new OperationContext()))
        }
    }

    static void makeAdditionalGetMoreCall(MongoNamespace namespace, ServerCursor serverCursor, AsyncConnection connection) {
        def callback = new FutureResultCallback<>()
        connection.commandAsync(namespace.databaseName,
                new BsonDocument('getMore', new BsonInt64(serverCursor.getId()))
                        .append('collection', new BsonString(namespace.getCollectionName())),
                new NoOpFieldNameValidator(), ReadPreference.primary(),
                new BsonDocumentCodec(),
                new StaticBindingContext(new NoOpSessionContext(), getServerApi(), IgnorableRequestContext.INSTANCE,
                        new OperationContext()),
                callback
        )
        makeAdditionalGetMoreCallHandleError(serverCursor) { callback.get() }
    }

    static void makeAdditionalGetMoreCallHandleError(ServerCursor serverCursor, Runnable runnable) {
        try {
            runnable.run()
        } catch (MongoCommandException e) {
            if (e.getErrorCode() == 43) {
                throw new MongoCursorNotFoundException(serverCursor.getId(), e.getResponse(), serverCursor.getAddress())
            } else {
                throw new MongoQueryException(e.getResponse(), e.getServerAddress())
            }
        }
    }
}
