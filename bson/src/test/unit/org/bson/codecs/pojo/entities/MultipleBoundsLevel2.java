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

public class MultipleBoundsLevel2<R, S> extends MultipleBoundsLevel3<S> {
    private R level2;

    public MultipleBoundsLevel2() {
        super();
    }

    public MultipleBoundsLevel2(final R level2, final S level3) {
        super(level3);
        this.level2 = level2;
    }

    public R getLevel2() {
        return level2;
    }

    public void setLevel2(final R level2) {
        this.level2 = level2;
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
        final MultipleBoundsLevel2<?, ?> that = (MultipleBoundsLevel2<?, ?>) o;
        return Objects.equals(level2, that.level2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), level2);
    }
}
