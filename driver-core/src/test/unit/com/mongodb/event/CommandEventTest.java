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

package com.mongodb.event;

import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.internal.IgnorableRequestContext;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CommandEventTest {

    @Test
    void shouldFailIfElapsedTimeIsNegative() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
                new CommandSucceededEvent(IgnorableRequestContext.INSTANCE, 1, 1,
                        new ConnectionDescription(new ServerId(new ClusterId(), new ServerAddress())), "test", "ping",
                        new BsonDocument("ok", new BsonInt32(1)), -1));
        assertEquals("state should be: elapsed time is not negative", e.getMessage());

        e = assertThrows(IllegalArgumentException.class, () ->
                new CommandFailedEvent(IgnorableRequestContext.INSTANCE, 1, 1,
                        new ConnectionDescription(new ServerId(new ClusterId(), new ServerAddress())), "test", "ping", -1,
                        new Throwable()));
        assertEquals("state should be: elapsed time is not negative", e.getMessage());
    }
}
