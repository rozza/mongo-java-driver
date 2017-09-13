/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.operation;

import com.mongodb.bulk.WriteRequest;
import com.mongodb.internal.validator.CollectibleDocumentFieldNameValidator;
import com.mongodb.internal.validator.MappedFieldNameValidator;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import com.mongodb.internal.validator.UpdateFieldNameValidator;
import org.bson.FieldNameValidator;

import java.util.HashMap;
import java.util.Map;

import static com.mongodb.bulk.WriteRequest.Type.INSERT;
import static com.mongodb.bulk.WriteRequest.Type.REPLACE;
import static com.mongodb.bulk.WriteRequest.Type.UPDATE;

class BulkWriteFieldValidator implements FieldNameValidator {
    private final FieldNameValidator wrapped;
    private WriteRequest writeRequest;

    BulkWriteFieldValidator(final WriteRequest.Type batchType) {
        if (batchType == INSERT) {
            Map<String, FieldNameValidator> map = new HashMap<String, FieldNameValidator>();
            map.put("documents", new CollectibleDocumentFieldNameValidator());
            this.wrapped = new MappedFieldNameValidator(new NoOpFieldNameValidator(), map);
        } else if (batchType == UPDATE || batchType == REPLACE) {
            Map<String, FieldNameValidator> rootMap = new HashMap<String, FieldNameValidator>();
            rootMap.put("updates", new UpdatesValidator());
            this.wrapped = new MappedFieldNameValidator(new NoOpFieldNameValidator(), rootMap);
        } else {
            this.wrapped = new NoOpFieldNameValidator();
        }
    }

    @Override
    public boolean validate(final String fieldName) {
        return wrapped.validate(fieldName);
    }

    @Override
    public FieldNameValidator getValidatorForField(final String fieldName) {
        return wrapped.getValidatorForField(fieldName);
    }

    public void setWriteRequest(final WriteRequest writeRequest) {
        this.writeRequest = writeRequest;
    }

    private class UpdatesValidator implements FieldNameValidator {

        @Override
        public boolean validate(final String fieldName) {
            return true;
        }

        @Override
        public FieldNameValidator getValidatorForField(final String fieldName) {
            if (!fieldName.equals("u")) {
                return new NoOpFieldNameValidator();
            }

            if (writeRequest.getType() == WriteRequest.Type.REPLACE) {
                return new CollectibleDocumentFieldNameValidator();
            } else {
                return new UpdateFieldNameValidator();
            }
        }
    }

}
