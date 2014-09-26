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

package com.mongodb.operation;

import com.mongodb.CommandFailureException;
import com.mongodb.CursorFlag;
import com.mongodb.Function;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.async.MongoFuture;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.SingleResultFuture;
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.AsyncWriteBinding;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadBinding;
import com.mongodb.binding.WriteBinding;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ServerDescription;
import com.mongodb.protocol.CommandProtocol;
import com.mongodb.protocol.Protocol;
import com.mongodb.protocol.message.NoOpFieldNameValidator;
import org.bson.BsonDocument;
import org.bson.FieldNameValidator;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;

import java.util.EnumSet;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.connection.ServerType.SHARD_ROUTER;
import static com.mongodb.operation.OperationHelper.IdentityTransformer;

final class CommandOperationHelper {

    /* Read Binding Helpers */

    static BsonDocument executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                                      final ReadBinding binding) {
        return executeWrappedCommandProtocol(database, command, new BsonDocumentCodec(), binding);
    }

    static <T> T executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                               final ReadBinding binding,
                                               final Function<BsonDocument, T> transformer) {
        return executeWrappedCommandProtocol(database, command, new BsonDocumentCodec(), binding, transformer);
    }

    static <T> T executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                               final Decoder<T> decoder, final ReadBinding binding) {
        return executeWrappedCommandProtocol(database, command, decoder, binding, new IdentityTransformer<T>());
    }

    static <D, T> T executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                                  final Decoder<D> decoder, final ReadBinding binding,
                                                  final Function<D, T> transformer) {
        ConnectionSource source = binding.getReadConnectionSource();
        try {
            return transformer.apply(executeWrappedCommandProtocol(database, command, decoder, source,
                                                                   binding.getReadPreference()));
        } finally {
            source.release();
        }
    }

    /* Write Binding Helpers */

    static BsonDocument executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                                      final WriteBinding binding) {
        return executeWrappedCommandProtocol(database, command, binding, new IdentityTransformer<BsonDocument>());
    }

    static <T> T executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                               final Decoder<T> decoder, final WriteBinding binding) {
        return executeWrappedCommandProtocol(database, command, decoder, binding, new IdentityTransformer<T>());
    }

    static <T> T executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                               final WriteBinding binding,
                                               final Function<BsonDocument, T> transformer) {
        return executeWrappedCommandProtocol(database, command, new BsonDocumentCodec(), binding, transformer);
    }

    static <D, T> T executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                                  final Decoder<D> decoder, final WriteBinding binding,
                                                  final Function<D, T> transformer) {
        return executeWrappedCommandProtocol(database, command, new NoOpFieldNameValidator(), decoder, binding,
                                             transformer);
    }

    static <D, T> T executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                                  final FieldNameValidator fieldNameValidator, final Decoder<D> decoder,
                                                  final WriteBinding binding, final Function<D, T> transformer) {
        ConnectionSource source = binding.getWriteConnectionSource();
        try {
            return transformer.apply(executeWrappedCommandProtocol(database, command, fieldNameValidator, decoder,
                                                                   source, primary()));
        } finally {
            source.release();
        }
    }

    /* Connection Source Helpers */

    static <T> T executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                               final Decoder<T> decoder, final ConnectionSource source,
                                               final ReadPreference readPreference) {
        return executeWrappedCommandProtocol(database, command, new NoOpFieldNameValidator(), decoder, source,
                                             readPreference);
    }

    static <T> T executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                               final FieldNameValidator fieldNameValidator,
                                               final Decoder<T> decoder,
                                               final ConnectionSource source, final ReadPreference readPreference) {
        Connection connection = source.getConnection();
        try {
            return executeWrappedCommandProtocol(database, command, fieldNameValidator, decoder, connection,
                                                 readPreference);
        } finally {
            connection.release();
        }
    }

    /* Connection Helpers */

    static BsonDocument executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                                      final Connection connection) {
        return executeWrappedCommandProtocol(database, command, new BsonDocumentCodec(), connection, primary());
    }

    static <T> T executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                               final Decoder<T> decoder, final Connection connection,
                                               final ReadPreference readPreference) {
        return executeWrappedCommandProtocol(database, command, new NoOpFieldNameValidator(), decoder, connection,
                                             readPreference);
    }

    static <T> T executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                               final Decoder<BsonDocument> decoder, final Connection connection,
                                               final Function<BsonDocument, T> transformer) {
        return executeWrappedCommandProtocol(database, command, decoder, connection, primary(), transformer);
    }

    static <T> T executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                               final Connection connection, final ReadPreference readPreference,
                                               final Function<BsonDocument, T> transformer) {
        return executeWrappedCommandProtocol(database, command, new BsonDocumentCodec(), connection, readPreference,
                                             transformer);
    }

    static <D, T> T executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                                  final Decoder<D> decoder, final Connection connection,
                                                  final ReadPreference readPreference,
                                                  final Function<D, T> transformer) {
        return executeWrappedCommandProtocol(database, command, new NoOpFieldNameValidator(), decoder, connection,
                                             readPreference, transformer);
    }

    static <T> T executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                               final FieldNameValidator fieldNameValidator, final Decoder<T> decoder,
                                               final Connection connection, final ReadPreference readPreference) {
        return executeWrappedCommandProtocol(database, command, fieldNameValidator, decoder, connection, readPreference,
                                             new IdentityTransformer<T>());
    }

    static <D, T> T executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                                  final FieldNameValidator fieldNameValidator, final Decoder<D> decoder,
                                                  final Connection connection, final ReadPreference readPreference,
                                                  final Function<D, T> transformer) {
        return transformer.apply(new CommandProtocol<D>(database, wrapCommand(command, readPreference,
                                                                              connection.getServerDescription()),
                                                        getQueryFlags(readPreference), fieldNameValidator, decoder)
                                 .execute(connection));
    }

    /* Async Read Binding Helpers */

    static MongoFuture<BsonDocument> executeWrappedCommandProtocolAsync(final String database,
                                                                        final BsonDocument command,
                                                                        final AsyncReadBinding binding) {
        return executeWrappedCommandProtocolAsync(database, command, new BsonDocumentCodec(), binding);
    }

    static <T> MongoFuture<T> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                 final Decoder<T> decoder,
                                                                 final AsyncReadBinding binding) {
        return executeWrappedCommandProtocolAsync(database, command, decoder, binding, new IdentityTransformer<T>());
    }

    static <T> MongoFuture<T> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                 final AsyncReadBinding binding,
                                                                 final Function<BsonDocument, T> transformer) {
        return executeWrappedCommandProtocolAsync(database, command, new BsonDocumentCodec(), binding, transformer);
    }

    static <D, T> MongoFuture<T> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                    final Decoder<D> decoder,
                                                                    final AsyncReadBinding binding,
                                                                    final Function<D, T> transformer) {
        SingleResultFuture<T> future = new SingleResultFuture<T>();
        binding.getReadConnectionSource().register(new CommandProtocolExecutingCallback<D, T>(database, command,
                new NoOpFieldNameValidator(), decoder, primary(), future, transformer));

        return future;
    }

    /* Async Write Binding Helpers */

    static MongoFuture<BsonDocument> executeWrappedCommandProtocolAsync(final String database,
                                                                        final BsonDocument command,
                                                                        final AsyncWriteBinding binding) {
        return executeWrappedCommandProtocolAsync(database, command, new BsonDocumentCodec(), binding);
    }

    static <T> MongoFuture<T> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                 final Decoder<T> decoder,
                                                                 final AsyncWriteBinding binding) {
        return executeWrappedCommandProtocolAsync(database, command, decoder, binding, new IdentityTransformer<T>());
    }

    static <T> MongoFuture<T> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                 final AsyncWriteBinding binding,
                                                                 final Function<BsonDocument, T> transformer) {
        return executeWrappedCommandProtocolAsync(database, command, new BsonDocumentCodec(), binding, transformer);
    }

    static <D, T> MongoFuture<T> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                    final Decoder<D> decoder,
                                                                    final AsyncWriteBinding binding,
                                                                    final Function<D, T> transformer) {
        return executeWrappedCommandProtocolAsync(database, command, new NoOpFieldNameValidator(), decoder, binding,
                                                  transformer);
    }

    static <D, T> MongoFuture<T> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                    final FieldNameValidator fieldNameValidator,
                                                                    final Decoder<D> decoder,
                                                                    final AsyncWriteBinding binding,
                                                                    final Function<D, T> transformer) {
        SingleResultFuture<T> future = new SingleResultFuture<T>();
        binding.getWriteConnectionSource().register(new CommandProtocolExecutingCallback<D, T>(database, command,
                                                                                               fieldNameValidator,
                                                                                               decoder, primary(),
                                                                                               future,
                                                                                               transformer));
        return future;
    }

    /* Async Connection Helpers */

    static MongoFuture<BsonDocument> executeWrappedCommandProtocolAsync(final String database,
                                                                        final BsonDocument command,
                                                                        final Connection connection) {
        return executeWrappedCommandProtocolAsync(database, command, new BsonDocumentCodec(), connection);
    }

    static <D, T> MongoFuture<T> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                    final Connection connection,
                                                                    final Function<BsonDocument, T> transformer) {
        return executeWrappedCommandProtocolAsync(database, command, new BsonDocumentCodec(), connection, primary(),
                                                  transformer);
    }

    static <T> MongoFuture<T> executeWrappedCommandProtocolAsync(final String database,
                                                                 final BsonDocument command,
                                                                 final Decoder<T> decoder,
                                                                 final Connection connection) {
        return executeWrappedCommandProtocolAsync(database, command, decoder, connection, primary(),
                                                  new IdentityTransformer<T>());
    }

    static <D, T> MongoFuture<T> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                    final Decoder<D> decoder,
                                                                    final Connection connection,
                                                                    final ReadPreference readPreference,
                                                                    final Function<D, T> transformer) {
        final SingleResultFuture<T> future = new SingleResultFuture<T>();
        new CommandProtocol<D>(database, wrapCommand(command, readPreference, connection.getServerDescription()),
                               getQueryFlags(readPreference), new NoOpFieldNameValidator(), decoder)
        .executeAsync(connection).register(new SingleResultCallback<D>() {
            @Override
            public void onResult(final D result, final MongoException e) {
                if (e != null) {
                    future.init(null, e);
                } else {
                    future.init(transformer.apply(result), null);
                }
            }
        });
        return future;
    }

    /* Misc operation helpers */

    static void rethrowIfNotNamespaceError(final CommandFailureException e) {
        rethrowIfNotNamespaceError(e, null);
    }

    static <T> T rethrowIfNotNamespaceError(final CommandFailureException e, final T defaultValue) {
        if (!e.getErrorMessage().contains("ns not found") && e.getErrorCode() != 26) {
            throw e;
        }
        return defaultValue;
    }

    static <T> MongoFuture<T> rethrowIfNotNamespaceError(final MongoFuture<T> future) {
        return rethrowIfNotNamespaceError(future, null);
    }

    static <T> MongoFuture<T> rethrowIfNotNamespaceError(final MongoFuture<T> future, final T defaultValue) {
        final SingleResultFuture<T> ignoringFuture = new SingleResultFuture<T>();
        future.register(new SingleResultCallback<T>() {
            @Override
            public void onResult(final T result, final MongoException e) {
                MongoException checkedError = e;
                // Check for a namespace error which we can safely ignore
                if (e instanceof CommandFailureException) {
                    try {
                        checkedError = null;
                        rethrowIfNotNamespaceError((CommandFailureException) e);
                    } catch (CommandFailureException error) {
                        checkedError = error;
                    }
                }
                if (result != null) {
                    ignoringFuture.init(result, checkedError);
                } else {
                    ignoringFuture.init(defaultValue, checkedError);
                }
            }
        });
        return ignoringFuture;
    }

    static BsonDocument wrapCommand(final BsonDocument command, final ReadPreference readPreference,
                                    final ServerDescription serverDescription) {
        if (serverDescription.getType() == SHARD_ROUTER && !readPreference.equals(primary())) {
            return new BsonDocument("$query", command).append("$readPreference", readPreference.toDocument());
        } else {
            return command;
        }
    }

    static EnumSet<CursorFlag> getQueryFlags(final ReadPreference readPreference) {
        if (readPreference.isSlaveOk()) {
            return EnumSet.of(CursorFlag.SLAVE_OK);
        } else {
            return EnumSet.noneOf(CursorFlag.class);
        }
    }

    private static class CommandProtocolExecutingCallback<D, R> implements SingleResultCallback<AsyncConnectionSource> {
        private final String database;
        private final BsonDocument command;
        private final Decoder<D> decoder;
        private final ReadPreference readPreference;
        private final FieldNameValidator fieldNameValidator;
        private final SingleResultFuture<R> future;
        private final Function<D, R> transformer;

        public CommandProtocolExecutingCallback(final String database, final BsonDocument command,
                                                final FieldNameValidator fieldNameValidator,
                                                final Decoder<D> decoder,
                                                final ReadPreference readPreference,
                                                final SingleResultFuture<R> future,
                                                final Function<D, R> transformer) {
            this.fieldNameValidator = fieldNameValidator;
            this.future = future;
            this.transformer = transformer;
            this.database = database;
            this.command = command;
            this.decoder = decoder;
            this.readPreference = readPreference;
        }

        protected Protocol<D> getProtocol(final ServerDescription serverDescription) {
            return new CommandProtocol<D>(database, wrapCommand(command, readPreference, serverDescription),
                                          getQueryFlags(readPreference), fieldNameValidator, decoder);
        }

        @Override
        public void onResult(final AsyncConnectionSource source, final MongoException e) {
            if (e != null) {
                future.init(null, e);
            } else {
                source.getConnection().register(new SingleResultCallback<Connection>() {
                    @Override
                    public void onResult(final Connection connection, final MongoException e) {
                        if (e != null) {
                            future.init(null, e);
                        } else {
                            getProtocol(connection.getServerDescription())
                            .executeAsync(connection)
                            .register(new SingleResultCallback<D>() {
                                @Override
                                public void onResult(final D response, final MongoException e) {
                                    try {
                                        if (e != null) {
                                            future.init(null, e);
                                        } else {
                                            future.init(transformer.apply(response), null);
                                        }
                                    } finally {
                                        connection.release();
                                        source.release();
                                    }
                                }
                            });
                        }
                    }
                });
            }
        }
    }

    private CommandOperationHelper() {
    }
}
