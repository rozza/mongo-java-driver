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

import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import org.bson.io.BsonInput;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class PlainAuthenticatorUnitTest {
    private TestInternalConnection connection;
    private MongoCredential credential;
    private PlainAuthenticator subject;

    @Before
    public void before() {
        connection = new TestInternalConnection(new ServerAddress("localhost", 27017));
        credential = MongoCredential.createPlainCredential("user", "$external", "pencil".toCharArray());
        subject = new PlainAuthenticator(this.credential, this.connection);
    }

    @Test
    public void testSuccessfulAuthentication() {
        ResponseBuffers reply = MessageHelper.buildSuccessfulReply(
                "{conversationId: 1, "
                        + "done: true, "
                        + "ok: 1}");

        connection.enqueueReply(reply);

        subject.authenticate();

        List<BsonInput> sent = connection.getSent();
        String command = MessageHelper.decodeCommandAsJson(sent.get(0));
        String expectedCommand = "{ \"saslStart\" : 1, "
                + "\"mechanism\" : \"PLAIN\", "
                + "\"payload\" : { \"$binary\" : \"dXNlcgB1c2VyAHBlbmNpbA==\", \"$type\" : \"0\" } }";

        assertEquals(expectedCommand, command);
    }
}
