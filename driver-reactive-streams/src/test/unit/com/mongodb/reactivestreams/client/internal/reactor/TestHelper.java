package com.mongodb.reactivestreams.client.internal.reactor;

import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.operation.AsyncReadOperation;
import com.mongodb.internal.operation.AsyncWriteOperation;
import org.bson.Document;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.spockframework.util.Nullable;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toMap;
import static org.junit.internal.Checks.notNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;

@SuppressWarnings("unchecked")
@ExtendWith(MockitoExtension.class)
public class TestHelper {

    @Mock
    AsyncBatchCursor<Document> batchCursor;

    void configureBatchCursor() {
        AtomicBoolean isClosed = new AtomicBoolean(false);
        Mockito.doAnswer(i -> isClosed.get()).when(batchCursor).isClosed();
        Mockito.doAnswer(invocation -> {
            isClosed.set(true);
            invocation.getArgument(0, SingleResultCallback.class).onResult(null, null);
            return null;
        }).when(batchCursor).next(any(SingleResultCallback.class));
    }

    public static void assertOperationIsTheSameAs(final Object expectedOperation, final Object actualOperation) {
        assertTrue(notNull(expectedOperation) instanceof  AsyncReadOperation || notNull(expectedOperation) instanceof AsyncWriteOperation,
                "Must be a read or write operation");
        if (expectedOperation instanceof AsyncReadOperation) {
            assertTrue(actualOperation instanceof AsyncReadOperation, "Both async read operations");
        } else {
            assertTrue(actualOperation instanceof AsyncWriteOperation, "Both async write operations");
        }

        Map<String, Object> expectedMap = getClassGetterValues(expectedOperation);
        Map<String, Object> actualMap = getClassGetterValues(actualOperation);
        assertEquals(expectedMap, actualMap);
    }

    @NotNull
    private static Map<String, Object> getClassGetterValues(final Object instance) {
        if (instance == null) {
            return emptyMap();
        }
        return Arrays.stream(instance.getClass().getMethods())
                .filter(n -> n.getParameterCount() == 0 && (n.getName().startsWith("get") || n.getName().startsWith("is")))
                .collect(toMap(Method::getName, n -> {
                    Object value = null;
                    try {
                        value = n.invoke(instance);
                    } catch (Exception e) {
                        // Exception
                    }
                    return value != null ? value : "null";
                }));
    }

    TestHelper() {
    }
}
