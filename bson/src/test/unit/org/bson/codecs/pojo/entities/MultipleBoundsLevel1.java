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

import java.util.Objects;

public class MultipleBoundsLevel1<T, R, S> extends MultipleBoundsLevel2<R, S> {
    private T level1;

    public MultipleBoundsLevel1() {
        super();
    }

    public MultipleBoundsLevel1(final T level1, final R level2, final S level3) {
        super(level2, level3);
        this.level1 = level1;
    }

    public T getLevel1() {
        return level1;
    }

    public void setLevel1(final T level1) {
        this.level1 = level1;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        MultipleBoundsLevel1<?, ?, ?> that = (MultipleBoundsLevel1<?, ?, ?>) o;
        return Objects.equals(level1, that.level1);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (level1 != null ? level1.hashCode() : 0);
        return result;
    }
}
