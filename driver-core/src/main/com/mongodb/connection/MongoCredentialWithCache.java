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

package com.mongodb.connection;

import com.mongodb.AuthenticationMechanism;
import com.mongodb.MongoCredential;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class MongoCredentialWithCache {
    private final MongoCredential credential;
    private final Map<Object, Object> cache;

    static List<MongoCredentialWithCache> create(final List<MongoCredential> credentialList) {
        Map<Object, Object> cache = Collections.synchronizedMap(new LinkedHashMap<Object, Object>() {
            private static final long serialVersionUID = 1L;
            @Override
            protected boolean removeEldestEntry(final Map.Entry<Object, Object> eldest) {
                return size() > credentialList.size();
            }
        });
        ArrayList<MongoCredentialWithCache> credentialListWithCache = new ArrayList<MongoCredentialWithCache>();
        for (MongoCredential credential : credentialList) {
            credentialListWithCache.add(new MongoCredentialWithCache(credential, cache));
        }
        return credentialListWithCache;
    }

    MongoCredentialWithCache(final MongoCredential credential, final Map<Object, Object> cache) {
        this.credential = credential;
        this.cache = cache;
    }

    MongoCredentialWithCache withMechanism(final AuthenticationMechanism mechanism) {
        return new MongoCredentialWithCache(credential.withMechanism(mechanism), cache);
    }

    AuthenticationMechanism getAuthenticationMechanism() {
        return credential.getAuthenticationMechanism();
    }

    public MongoCredential getCredential() {
        return credential;
    }

    @SuppressWarnings("unchecked")
    <T> T getFromCache(final Object key, final Class<T> clazz) {
        if (cache != null) {
            return clazz.cast(cache.get(key));
        }
        return null;
    }

    void putInCache(final Object key, final Object value) {
        if (cache != null) {
            cache.put(key, value);
        }
    }
}
