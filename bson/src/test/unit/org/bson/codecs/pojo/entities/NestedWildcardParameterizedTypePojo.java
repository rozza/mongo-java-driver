/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.bson.codecs.pojo.entities;

import java.util.List;
import java.util.Objects;

public class NestedWildcardParameterizedTypePojo {

    private List<? extends NestedWildcardParameterizedTypeField<? extends NestedWildcardParameterizedTypeNestedField<Integer>>> valueList;


    public NestedWildcardParameterizedTypePojo() {
    }

    public NestedWildcardParameterizedTypePojo(final List<? extends NestedWildcardParameterizedTypeField<? extends NestedWildcardParameterizedTypeNestedField<Integer>>> value) {
        this.valueList = value;
    }

    public List<? extends NestedWildcardParameterizedTypeField<? extends NestedWildcardParameterizedTypeNestedField<Integer>>> getValueList() {
        return valueList;
    }

    public void setValueList(final List<? extends NestedWildcardParameterizedTypeField<? extends NestedWildcardParameterizedTypeNestedField<Integer>>> valueList) {
        this.valueList = valueList;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o){
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final NestedWildcardParameterizedTypePojo that = (NestedWildcardParameterizedTypePojo) o;
        return Objects.equals(valueList, that.valueList);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(valueList);
    }
}


