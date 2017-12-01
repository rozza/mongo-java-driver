/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bson.codecs.pojo.entities;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ImmutableFinalMapAndCollectionsModel {

    private final List<Integer> listField;
    private final Map<String, Integer> mapField;

    public ImmutableFinalMapAndCollectionsModel() {
        this(Collections.<Integer>emptyList(), Collections.<String, Integer>emptyMap());
    }

    public ImmutableFinalMapAndCollectionsModel(final List<Integer> listField, final Map<String, Integer> mapField) {
        this.listField = Collections.unmodifiableList(listField);
        this.mapField = Collections.unmodifiableMap(mapField);
    }

    public List<Integer> getListField() {
        return listField;
    }

    public Map<String, Integer> getMapField() {
        return mapField;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()){
            return false;
        }

        ImmutableFinalMapAndCollectionsModel that = (ImmutableFinalMapAndCollectionsModel) o;

        if (listField != null ? !listField.equals(that.listField) : that.listField != null) {
            return false;
        }
        return mapField != null ? mapField.equals(that.mapField) : that.mapField == null;
    }

    @Override
    public int hashCode() {
        int result = listField != null ? listField.hashCode() : 0;
        result = 31 * result + (mapField != null ? mapField.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ImmutableFinalMapAndCollectionsModel{"
                + "listField=" + listField
                + ", mapField=" + mapField
                + '}';
    }
}
