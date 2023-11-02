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
import com.mongodb.internal.binding.ConnectionSource
import com.mongodb.internal.connection.Connection
import com.mongodb.internal.connection.OperationContext
import com.mongodb.internal.validator.NoOpFieldNameValidator
import org.bson.BsonDocument
import org.bson.BsonInt64
import org.bson.BsonString
import org.bson.codecs.BsonDocumentCodec

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

    static void makeAdditionalGetMoreCall(MongoNamespace namespace, ServerCursor serverCursor,
            ConnectionSource connectionSource) {
        def connection = connectionSource.getConnection()
        try {
            makeAdditionalGetMoreCall(namespace, serverCursor, connection, connectionSource.operationContext)
        } finally {
            connection.release()
        }
    }

    static void makeAdditionalGetMoreCall(MongoNamespace namespace, ServerCursor serverCursor, Connection connection,
            OperationContext operationContext) {
        try {
            connection.command(namespace.databaseName,
                    new BsonDocument('getMore', new BsonInt64(serverCursor.getId()))
                            .append('collection', new BsonString(namespace.getCollectionName())),
                    new NoOpFieldNameValidator(), ReadPreference.primary(),
                    new BsonDocumentCodec(),
                    operationContext)
        } catch (MongoCommandException e) {
            if (e.getErrorCode() == 43) {
                throw new MongoCursorNotFoundException(serverCursor.getId(), e.getResponse(), serverCursor.getAddress())
            } else {
                throw new MongoQueryException(e.getResponse(), e.getServerAddress())
            }
        }
    }
}
