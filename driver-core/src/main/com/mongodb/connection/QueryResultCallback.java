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

import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.ServerAddress;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import org.bson.Document;
import org.bson.codecs.Decoder;
import org.bson.codecs.DocumentCodec;

import static com.mongodb.connection.ProtocolHelper.getQueryFailureException;
import static java.lang.String.format;

class QueryResultCallback<T> extends ResponseCallback {
    public static final Logger LOGGER = Loggers.getLogger("protocol.query");

    private final SingleResultCallback<QueryResult<T>> callback;
    private final Decoder<T> decoder;

    public QueryResultCallback(final SingleResultCallback<QueryResult<T>> callback, final Decoder<T> decoder,
                               final int requestId, final ServerAddress serverAddress) {
        super(requestId, serverAddress);
        this.callback = callback;
        this.decoder = decoder;
    }

    @Override
    protected boolean callCallback(final ResponseBuffers responseBuffers, final Throwable t) {
        QueryResult<T> result = null;
        MongoException exceptionResult = null;
        try {
            if (t != null) {
                throw t;
            } else if (responseBuffers.getReplyHeader().isQueryFailure()) {
                Document errorDocument = new ReplyMessage<Document>(responseBuffers, new DocumentCodec(),
                                                                    getRequestId()).getDocuments().get(0);
                throw getQueryFailureException(getServerAddress(), errorDocument);
            } else {
                result = new QueryResult<T>(new ReplyMessage<T>(responseBuffers, decoder, getRequestId()), getServerAddress());
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(format("Query results received %s documents with cursor %s",
                                        result.getResults().size(),
                                        result.getCursor()));
                }
            }
        } catch (MongoException me) {
            exceptionResult = me;
        } catch (Throwable tr) {
            exceptionResult = new MongoInternalException("Internal exception", tr);
        } finally {
            if (responseBuffers != null) {
                responseBuffers.close();
            }
        }
        callback.onResult(result, exceptionResult);
        return true;
    }
}
