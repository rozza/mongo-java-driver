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
package com.mongodb.internal.async;

import com.mongodb.internal.async.function.RetryState;
import com.mongodb.internal.async.function.RetryingAsyncCallbackSupplier;

import java.util.function.Function;
import java.util.function.Predicate;

@FunctionalInterface
public interface Mono<T> {

    /**
     * @return an empty Mono
     */
    static Mono<Void> empty() {
        return (c) -> c.complete(c);
    }

    /**
     * Create a Mono from a value
     *
     * @param value the value to return in the Mono
     * @param <R> the type of the value
     * @return a Mono containing a value
     */
    static <R> Mono<R> from(final R value) {
        return (c) -> c.complete(value);
    }

    /**
     * Converts the results of the Mono to another type
     *
     * @param function The function to apply to the result of the Mono
     * @param <R> The return type of the resulting supplier
     * @return a new Mono with the previous results mapped to a new type
     */
    default <R> Mono<R> map(final Function<T, R> function) {
        return (c) -> this.unsafeFinish((v, e) -> {
            if (e == null) {
                c.complete(function.apply(v));
            } else {
                c.completeExceptionally(e);
            }
        });
    }

    /**
     * Transform the item emitted by this Mono returning the value emitted by another Mono (possibly changing the value type).
     *
     * @param <R> The return type of the resulting Mono
     * @return the transformed value in a Mono.
     */
    default <R> Mono<R> flatMap(Function<T, Mono<R>> transformer) {
        return (c) -> this.unsafeFinish((v, e) -> {
            if (e != null) {
                c.completeExceptionally(e);
            } else {
                transformer.apply(v).unsafeFinish(c);
            }
        });
    }

    /**
     * Chains Mono's together, ignoring the previous result of the Mono and returns a new Mono
     *
     * @param mono the next action
     * @return the composition
     */
    default Mono<T> then(final Mono<T> mono) {
        return flatMap(x -> mono);
    }

    /**
     * Subscribe to a fallback Mono when any error occurs, using a function to choose the fallback depending on the error.
     *
     * @param fallbackFunction the function that produces the fallback Mono
     * @return a fall
     */
    default Mono<T> onErrorResume(Function<? super Throwable, Mono<T>> fallbackFunction) {
        return (callback) -> this.finish((r, e) -> {
            if (e == null) {
                //noinspection DataFlowIssue
                callback.complete(r);
            } else {
                try {
                    fallbackFunction.apply(e).unsafeFinish(callback);
                } catch (Throwable t) {
                    t.addSuppressed(e);
                    callback.completeExceptionally(t);
                }
            }
        });
    }

    /**
     * Loop until a condition is met
     *
     * @param mono the loop mono
     * @param shouldRetry condition under which to retry
     * @return the composition of this, and the looping branch
     * @see RetryingAsyncCallbackSupplier
     */
    default Mono<T> doWhile(final Mono<T> mono, final Predicate<Throwable> shouldRetry) {
        return c ->
            new RetryingAsyncCallbackSupplier<T>(
                    new RetryState(),
                    (previouslyChosenFailure, lastAttemptFailure) -> lastAttemptFailure,
                    (rs, lastAttemptFailure) -> shouldRetry.test(lastAttemptFailure),
                    mono::finish // finish is required here, to handle exceptions
            ).get(c);
    }

    /**
     * Must be invoked at end of async chain.
     * @param callback the callback provided by the method the chain is used in
     */
    default void finish(final SingleResultCallback<T> callback) {
        final boolean[] callbackInvoked = {false};
        try {
            this.unsafeFinish((v, e) -> {
                callbackInvoked[0] = true;
                callback.onResult(v, e);
            });
        } catch (Throwable t) {
            if (callbackInvoked[0]) {
                throw t;
            } else {
                callback.completeExceptionally(t);
            }
        }
    }

    /**
     * This should not be called externally to this API. It should be
     * implemented as a lambda. To "finish" an async chain, use one of
     * the "finish" methods.
     *
     * @see #finish(SingleResultCallback)
     */
    void unsafeFinish(final SingleResultCallback<T> callback);

}
