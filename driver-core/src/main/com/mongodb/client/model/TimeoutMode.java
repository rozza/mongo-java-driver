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

package com.mongodb.client.model;

/**
 * The timeout mode sets the scope of the timeout for any cursor based iterables and publishers.
 *
 * @since 4.x
 */
public enum TimeoutMode {

    /**
     * The default state.
     *
     * <p>When {@code timeoutMS} is set and no explicit configuration of TimeoutMode is set then:
     * <ul>
     *     <li>{@link com.mongodb.CursorType#NonTailable} cursors default to: {@link TimeoutMode#CURSOR_LIFETIME}.</li>
     *     <li>{@link com.mongodb.CursorType#Tailable} cursors default to: {@link TimeoutMode#ITERATION}.</li>
     *     <li>{@link com.mongodb.CursorType#TailableAwait} cursors default to: {@link TimeoutMode#CURSOR_LIFETIME}.</li>
     * </ul>
     * </p>
     */
    DEFAULT,

    /**
     * The {@code timeoutMS} is applied to the whole lifetime of the cursor
     *
     * <p>Under this mode the {@code timeoutMS} will also apply to each {@code getMore} call.</p>
     */
    CURSOR_LIFETIME,

    /**
     * The {@code timeoutMS} is refreshed for each stage of a cursor.
     *
     * <p>Under this mode the {@code timeoutMS} will reset for each {@code getMore} call.</p>
     */
    ITERATION
}
