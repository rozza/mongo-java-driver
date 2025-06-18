package com.mongodb.internal.operation;

import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.WriteBinding;

public class NamedWriteOperation<T> implements AsyncWriteOperation<T>, WriteOperation<T> {
    final String operationName;
    final WriteOperation<T> writeOperation;
    final AsyncWriteOperation<T> asyncWriteOperation;

    public static <T> NamedWriteOperation<T> createNamedWriteOperation(final String operationName, final WriteOperation<T> writeOperation,
            final AsyncWriteOperation<T> asyncWriteOperation) {
        return new NamedWriteOperation<T>(operationName, writeOperation, asyncWriteOperation);
    }

    private NamedWriteOperation(final String operationName, final WriteOperation<T> writeOperation,
            final AsyncWriteOperation<T> asyncWriteOperation) {
        this.operationName = operationName;
        this.writeOperation = writeOperation;
        this.asyncWriteOperation = asyncWriteOperation;
    }

    public String getOperationName() {
        return operationName;
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<T> callback) {
        asyncWriteOperation.executeAsync(binding, callback);
    }

    @Override
    public T execute(final WriteBinding binding) {
        return writeOperation.execute(binding);
    }
}
