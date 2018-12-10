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

package org.bson.codecs.pojo.entities;

import java.util.Map;

public class SimpleEnumMapModel {

    private Map<SimpleEnum, String> simpleEnumStringMap;

    public SimpleEnumMapModel() {
    }

    public SimpleEnumMapModel(final Map<SimpleEnum, String> simpleEnumStringMap) {
        this.simpleEnumStringMap = simpleEnumStringMap;
    }

    public Map<SimpleEnum, String> getSimpleEnumStringMap() {
        return simpleEnumStringMap;
    }

    public void setSimpleEnumStringMap(final Map<SimpleEnum, String> simpleEnumStringMap) {
        this.simpleEnumStringMap = simpleEnumStringMap;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SimpleEnumMapModel that = (SimpleEnumMapModel) o;

        return simpleEnumStringMap != null ? simpleEnumStringMap.equals(that.simpleEnumStringMap) : that.simpleEnumStringMap == null;
    }

    @Override
    public int hashCode() {
        return simpleEnumStringMap != null ? simpleEnumStringMap.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "SimpleEnumMapModel{"
                + "simpleEnumStringMap=" + simpleEnumStringMap
                + '}';
    }
}
