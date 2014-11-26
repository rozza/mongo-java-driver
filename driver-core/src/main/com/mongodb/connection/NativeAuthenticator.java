/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.connection;

import com.mongodb.CommandFailureException;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.MongoSecurityException;
import com.mongodb.async.SingleResultCallback;
import org.bson.BsonDocument;
import org.bson.BsonString;

import static com.mongodb.connection.CommandHelper.executeCommand;
import static com.mongodb.connection.CommandHelper.executeCommandAsync;
import static com.mongodb.connection.NativeAuthenticationHelper.getAuthCommand;
import static com.mongodb.connection.NativeAuthenticationHelper.getNonceCommand;

class NativeAuthenticator extends Authenticator {
    public NativeAuthenticator(final MongoCredential credential) {
        super(credential);
    }

    @Override
    public void authenticate(final InternalConnection connection, final ConnectionDescription connectionDescription) {
        try {
            BsonDocument nonceResponse = executeCommand(getCredential().getSource(),
                                                         getNonceCommand(),
                                                         connection);

            BsonDocument authCommand = getAuthCommand(getCredential().getUserName(),
                                                      getCredential().getPassword(),
                                                      ((BsonString) nonceResponse.get("nonce")).getValue());
            executeCommand(getCredential().getSource(), authCommand, connection);
        } catch (CommandFailureException e) {
            throw new MongoSecurityException(getCredential(), "Exception authenticating", e);
        }
    }

    @Override
    void authenticateAsync(final InternalConnection connection, final ConnectionDescription connectionDescription,
                           final SingleResultCallback<Void> callback) {
        executeCommandAsync(getCredential().getSource(), getNonceCommand(), connection,
                            new SingleResultCallback<BsonDocument>() {
                                @Override
                                public void onResult(final BsonDocument nonceResult, final MongoException e) {
                                    if (e != null) {
                                        callback.onResult(null, translateException(e));
                                    } else {
                                        executeCommandAsync(getCredential().getSource(),
                                                            getAuthCommand(getCredential().getUserName(), getCredential().getPassword(),
                                                                           ((BsonString) nonceResult.get("nonce")).getValue()),
                                                            connection,
                                                            new SingleResultCallback<BsonDocument>() {
                                                                @Override
                                                                public void onResult(final BsonDocument result, final MongoException e) {
                                                                    if (e != null) {
                                                                        callback.onResult(null, translateException(e));
                                                                    } else {
                                                                        callback.onResult(null, null);
                                                                    }
                                                                }
                                                            });
                                    }
                                }
                            });
    }

    private MongoException translateException(final MongoException e) {
        if (e instanceof CommandFailureException) {
            return new MongoSecurityException(getCredential(), "Exception authenticating", e);
        } else {
            return e;
        }
    }
}
