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

package com.mongodb.client.model;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * The options to apply to a find operation (also commonly referred to as a query).
 *
 * @since 3.0
 * @mongodb.driver.manual manual/tutorial/query-documents/ Find
 * @mongodb.driver.manual meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
 */
public final class FindOptions {
    private int batchSize;
    private int limit;
    private Object modifiers;
    private Object projection;
    private long maxTimeMS;
    private int skip;
    private Object sort;
    private boolean awaitData;
    private boolean exhaust;
    private boolean noCursorTimeout;
    private boolean oplogReplay;
    private boolean partial;
    private boolean tailable;

    /**
     * Construct a new instance.
     */
    public FindOptions() {
    }

    /**
     * Construct a new instance by making a shallow copy of the given model.
     * @param from model to copy
     */
    public FindOptions(final FindOptions from) {
        batchSize = from.batchSize;
        limit = from.limit;
        modifiers = from.modifiers;
        projection = from.projection;
        maxTimeMS = from.maxTimeMS;
        skip = from.skip;
        sort = from.sort;
        awaitData = from.awaitData;
        exhaust = from.exhaust;
        noCursorTimeout = from.noCursorTimeout;
        oplogReplay = from.oplogReplay;
        partial = from.partial;
        tailable = from.tailable;
    }

    /**
     * Gets the limit to apply.  The default is null.
     *
     * @return the limit
     * @mongodb.driver.manual manual/reference/method/cursor.limit/#cursor.limit Limit
     */
    public int getLimit() {
        return limit;
    }

    /**
     * Sets the limit to apply.
     *
     * @param limit the limit, which may be null
     * @return this
     * @mongodb.driver.manual manual/reference/method/cursor.limit/#cursor.limit Limit
     */
    public FindOptions limit(final int limit) {
        this.limit = limit;
        return this;
    }

    /**
     * Gets the number of documents to skip.  The default is 0.
     *
     * @return the number of documents to skip, which may be null
     * @mongodb.driver.manual manual/reference/method/cursor.skip/#cursor.skip Skip
     */
    public int getSkip() {
        return skip;
    }

    /**
     * Sets the number of documents to skip.
     *
     * @param skip the number of documents to skip
     * @return this
     * @mongodb.driver.manual manual/reference/method/cursor.skip/#cursor.skip Skip
     */
    public FindOptions skip(final int skip) {
        this.skip = skip;
        return this;
    }

    /**
     * Gets the maximum execution time on the server for this operation.  The default is 0, which places no limit on the execution time.
     *
     * @param timeUnit the time unit to return the result in
     * @return the maximum execution time in the given time unit
     * @mongodb.driver.manual manual/reference/method/cursor.maxTimeMS/#cursor.maxTimeMS Max Time
     */
    public long getMaxTime(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        return timeUnit.convert(maxTimeMS, TimeUnit.MILLISECONDS);
    }

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime  the max time
     * @param timeUnit the time unit, which may not be null
     * @return this
     * @mongodb.driver.manual manual/reference/method/cursor.maxTimeMS/#cursor.maxTimeMS Max Time
     */
    public FindOptions maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    /**
     * Gets the number of documents to return per batch.  Default to 0, which indicates that the server chooses an appropriate batch
     * size.
     *
     * @return the batch size, which may be null
     * @mongodb.driver.manual manual/reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size
     * @return this
     * @mongodb.driver.manual manual/reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    public FindOptions batchSize(final int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    /**
     * Gets the query modifiers to apply to this operation.  The default is not to apply any modifiers.
     *
     * @return the query modifiers, which may be null
     * @mongodb.driver.manual manual/reference/operator/query-modifier/ Query Modifiers
     */
    public Object getModifiers() {
        return modifiers;
    }

    /**
     * Sets the query modifiers to apply to this operation.
     *
     * @param modifiers the query modifiers to apply, which may be null.
     * @return this
     * @mongodb.driver.manual manual/reference/operator/query-modifier/ Query Modifiers
     */
    public FindOptions modifiers(final Object modifiers) {
        this.modifiers = modifiers;
        return this;
    }

    /**
     * Gets a document describing the fields to return for all matching documents.
     *
     * @return the project document, which may be null
     * @mongodb.driver.manual manual/reference/method/db.collection.find/ Projection
     */
    public Object getProjection() {
        return projection;
    }

    /**
     * Sets a document describing the fields to return for all matching documents.
     *
     * @param projection the project document, which may be null.
     * @return this
     * @mongodb.driver.manual manual/reference/method/db.collection.find/ Projection
     */
    public FindOptions projection(final Object projection) {
        this.projection = projection;
        return this;
    }

    /**
     * Gets the sort criteria to apply to the query. The default is null, which means that the documents will be returned in an undefined
     * order.
     *
     * @return a document describing the sort criteria
     * @mongodb.driver.manual manual/reference/method/cursor.sort/ Sort
     */
    public Object getSort() {
        return sort;
    }

    /**
     * Sets the sort criteria to apply to the query.
     *
     * @param sort the sort criteria, which may be null.
     * @return this
     * @mongodb.driver.manual manual/reference/method/cursor.sort/ Sort
     */
    public FindOptions sort(final Object sort) {
        this.sort = sort;
        return this;
    }

    /**
     * Use with the tailable property. If there are no more matching documents, the server will block for a
     * while rather than returning no documents.
     *
     * @return whether the cursor will wait for more documents that match the criteria
     * @see com.mongodb.client.model.FindOptions#isTailable()
     */
    public boolean isAwaitData() {
        return awaitData;
    }

    /**
     * Use with the tailable property. If there are no more matching documents, the server will block for a
     * while rather than returning no documents.
     *
     * @param awaitData whether the cursor will wait for more documents that match the criteria
     * @return this
     */
    public FindOptions awaitData(final boolean awaitData) {
        this.awaitData = awaitData;
        return this;
    }

    /**
     * If true, stream the data down full blast in multiple "more" packages, on the assumption that
     * the client will fully read all data queried. Faster when you are pulling a lot of
     * data and know you want to pull it all down, but note that it is not currently supported in sharded clusters.
     *
     * @return whether exhaust mode is enabled
     */
    public boolean isExhaust() {
        return exhaust;
    }

    /**
     * If true, stream the data down full blast in multiple "more" packages, on the assumption that
     * the client will fully read all data queried. Faster when you are pulling a lot of
     * data and know you want to pull it all down, but note that it is not currently supported in sharded clusters.
     *
     * @param exhaust whether exhaust mode is enabled
     * @return this
     */
    public FindOptions exhaust(final boolean exhaust) {
        this.exhaust = exhaust;
        return this;
    }

    /**
     * The server normally times out idle cursors after an inactivity period (10 minutes)
     * to prevent excess memory use.  If true, that timeout is disabled.
     *
     * @return true if cursor timeout is disabled
     */
    public boolean isNoCursorTimeout() {
        return noCursorTimeout;
    }

    /**
     * The server normally times out idle cursors after an inactivity period (10 minutes)
     * to prevent excess memory use. Set this option to prevent that.
     *
     * @param noCursorTimeout true if cursor timeout is disabled
     * @return this
     */
    public FindOptions noCursorTimeout(final boolean noCursorTimeout) {
        this.noCursorTimeout = noCursorTimeout;
        return this;
    }

    /**
     * Users should not set this under normal circumstances.
     *
     * @return if oplog replay is enabled
     */
    public boolean isOplogReplay() {
        return oplogReplay;
    }

    /**
     * Users should not set this under normal circumstances.
     *
     * @param oplogReplay if oplog replay is enabled
     * @return this
     */
    public FindOptions oplogReplay(final boolean oplogReplay) {
        this.oplogReplay = oplogReplay;
        return this;
    }

    /**
     * Get partial results from a sharded cluster if one or more shards are unreachable (instead of throwing an error).
     *
     * @return if partial results for sharded clusters is enabled
     */
    public boolean isPartial() {
        return partial;
    }

    /**
     * Get partial results from a sharded cluster if one or more shards are unreachable (instead of throwing an error).
     *
     * @param partial if partial results for sharded clusters is enabled
     * @return this
     */
    public FindOptions partial(final boolean partial) {
        this.partial = partial;
        return this;
    }

    /**
     * Tailable means the cursor is not closed when the last data is retrieved.
     * Rather, the cursor marks the final documents's position. You can resume
     * using the cursor later, from where it was located, if more data were
     * received. Like any "latent cursor", the cursor may become invalid at
     * some point - for example if the final document it references is deleted.
     *
     * @return true if tailable is enabled
     */
    public boolean isTailable() {
        return tailable;
    }

    /**
     * Tailable means the cursor is not closed when the last data is retrieved.
     * Rather, the cursor marks the final documents's position. You can resume
     * using the cursor later, from where it was located, if more data were
     * received. Like any "latent cursor", the cursor may become invalid at
     * some point - for example if the final document it references is deleted.
     * *
     * @param tailable if tailable is enabled
     * @return this
     */
    public FindOptions tailable(final boolean tailable) {
        this.tailable = tailable;
        return this;
    }

    @Override
    public String toString() {
        return "FindOptions{"
               + ", batchSize=" + batchSize
               + ", limit=" + limit
               + ", modifiers=" + modifiers
               + ", projection=" + projection
               + ", maxTimeMS=" + maxTimeMS
               + ", skip=" + skip
               + ", sort=" + sort
               + ", awaitData=" + awaitData
               + ", exhaust=" + exhaust
               + ", noCursorTimeout=" + noCursorTimeout
               + ", oplogReplay=" + oplogReplay
               + ", partial=" + partial
               + ", tailable=" + tailable
               + '}';
    }
}
