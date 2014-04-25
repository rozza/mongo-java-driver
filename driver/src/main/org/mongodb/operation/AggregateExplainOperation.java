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

package org.mongodb.operation;

import org.mongodb.AggregationOptions;
import org.mongodb.CommandResult;
import org.mongodb.Document;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.binding.AsyncReadBinding;
import org.mongodb.binding.ReadBinding;

import java.util.List;

import static org.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static org.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;

/**
 * An operation that executes an explain on an aggregation pipeline.
 *
 * @since 3.0
 */
public class AggregateExplainOperation implements AsyncReadOperation<CommandResult>, ReadOperation<CommandResult> {
    private final MongoNamespace namespace;
    private final List<Document> pipeline;
    private final AggregationOptions options;

    /**
     * Constructs a new instance.
     * @param namespace the namespace
     * @param pipeline the aggregation pipeline
     * @param options the aggregation options
     */
    public AggregateExplainOperation(final MongoNamespace namespace, final List<Document> pipeline, final AggregationOptions options) {
        this.namespace = namespace;
        this.pipeline = pipeline;
        this.options = options;
    }

    @Override
    public CommandResult execute(final ReadBinding binding) {
        return executeWrappedCommandProtocol(namespace, getCommand(), binding);
    }

    @Override
    public MongoFuture<CommandResult> executeAsync(final AsyncReadBinding binding) {
        return executeWrappedCommandProtocolAsync(namespace, getCommand(), binding);
    }

    private Document getCommand() {
        Document command = AggregateHelper.asCommandDocument(namespace, pipeline, options);
        command.put("explain", true);
        return command;
    }

}
