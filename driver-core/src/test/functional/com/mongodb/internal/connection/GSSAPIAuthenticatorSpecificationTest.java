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

package com.mongodb.internal.connection;

import com.mongodb.ClusterFixture;
import com.mongodb.LoggerSettings;
import com.mongodb.MongoCompressor;
import com.mongodb.MongoCredential;
import com.mongodb.MongoDriverInformation;
import com.mongodb.SubjectProvider;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.SocketSettings;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import java.util.Collections;

import static com.mongodb.AuthenticationMechanism.GSSAPI;
import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static com.mongodb.ClusterFixture.getLoginContextName;
import static com.mongodb.ClusterFixture.getPrimary;
import static com.mongodb.ClusterFixture.getServerApi;
import static com.mongodb.ClusterFixture.getSslSettings;
import static com.mongodb.MongoCredential.JAVA_SUBJECT_PROVIDER_KEY;
import static com.mongodb.connection.ClusterConnectionMode.SINGLE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class GSSAPIAuthenticatorSpecificationTest {

    @BeforeAll
    static void beforeAll() {
        assumeTrue(ClusterFixture.getCredential() != null
                && ClusterFixture.getCredential().getAuthenticationMechanism() == GSSAPI);
    }

    @Test
    void shouldUseSubjectProviderMechanismProperty() throws Exception {
        LoginContext loginContext = new LoginContext(getLoginContextName());
        loginContext.login();
        Subject subject = loginContext.getSubject();

        SubjectProvider subjectProvider = mock(SubjectProvider.class);
        when(subjectProvider.getSubject()).thenReturn(subject);

        MongoCredential credential = ClusterFixture.getCredential()
                .withMechanismProperty(JAVA_SUBJECT_PROVIDER_KEY, subjectProvider);
        MongoCredentialWithCache credentialWithCache = new MongoCredentialWithCache(credential);
        SocketStreamFactory streamFactory = new SocketStreamFactory(new DefaultInetAddressResolver(),
                SocketSettings.builder().build(), getSslSettings());
        InternalConnection internalConnection = new InternalStreamConnectionFactory(
                SINGLE, streamFactory,
                credentialWithCache,
                new ClientMetadata("test", MongoDriverInformation.builder().build()),
                Collections.<MongoCompressor>emptyList(),
                LoggerSettings.builder().build(), null, getServerApi())
                .create(new ServerId(new ClusterId(), getPrimary()));

        internalConnection.open(OPERATION_CONTEXT);

        verify(subjectProvider).getSubject();
    }
}
