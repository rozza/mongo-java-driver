package com.mongodb.reactivestreams.client.internal.reactor;

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoClientException;
import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.internal.async.client.AsyncClientSession;
import com.mongodb.internal.async.client.OperationExecutor;
import com.mongodb.internal.operation.AbortTransactionOperation;
import com.mongodb.internal.operation.CommitTransactionOperation;
import com.mongodb.internal.session.BaseClientSessionImpl;
import com.mongodb.internal.session.ServerSessionPool;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.MongoClient;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import static com.mongodb.MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL;
import static com.mongodb.MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL;
import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ClientSessionPublisherImpl extends BaseClientSessionImpl implements ClientSession {

    private enum TransactionState {
        NONE, IN, COMMITTED, ABORTED
    }
    private final OperationExecutor executor;
    private TransactionState transactionState = TransactionState.NONE;
    private boolean messageSentInCurrentTransaction;
    private boolean commitInProgress;
    private TransactionOptions transactionOptions;

    public ClientSessionPublisherImpl(final ServerSessionPool serverSessionPool, final MongoClient mongoClient,
                           final ClientSessionOptions options, final OperationExecutor executor) {
        super(serverSessionPool, mongoClient, options);
        this.executor = executor;
    }

    @Override
    public boolean hasActiveTransaction() {
        return transactionState == TransactionState.IN || (transactionState == TransactionState.COMMITTED && commitInProgress);
    }

    @Override
    public boolean notifyMessageSent() {
        if (hasActiveTransaction()) {
            boolean firstMessageInCurrentTransaction = !messageSentInCurrentTransaction;
            messageSentInCurrentTransaction = true;
            return firstMessageInCurrentTransaction;
        } else {
            if (transactionState == TransactionState.COMMITTED || transactionState == TransactionState.ABORTED) {
                cleanupTransaction(TransactionState.NONE);
            }
            return false;
        }
    }

    @Override
    public TransactionOptions getTransactionOptions() {
        isTrue("in transaction", transactionState == TransactionState.IN || transactionState == TransactionState.COMMITTED);
        return transactionOptions;
    }

    @Override
    public void startTransaction() {
        startTransaction(TransactionOptions.builder().build());
    }

    @Override
    public void startTransaction(final TransactionOptions transactionOptions) {
        notNull("transactionOptions", transactionOptions);
        if (transactionState == TransactionState.IN) {
            throw new IllegalStateException("Transaction already in progress");
        }
        if (transactionState == TransactionState.COMMITTED) {
            cleanupTransaction(TransactionState.IN);
        } else {
            transactionState = TransactionState.IN;
        }
        getServerSession().advanceTransactionNumber();
        this.transactionOptions = TransactionOptions.merge(transactionOptions, getOptions().getDefaultTransactionOptions());
        WriteConcern writeConcern = this.transactionOptions.getWriteConcern();
        if (writeConcern == null) {
            throw new MongoInternalException("Invariant violated. Transaction options write concern can not be null");
        }
        if (!writeConcern.isAcknowledged()) {
            throw new MongoClientException("Transactions do not support unacknowledged write concern");
        }
        setPinnedServerAddress(null);
    }


    @Override
    public AsyncClientSession getWrapped() {
        return null;
    }

    @Override
    public Publisher<Void> commitTransaction() {
        if (transactionState == TransactionState.ABORTED) {
            throw new IllegalStateException("Cannot call commitTransaction after calling abortTransaction");
        }
        if (transactionState == TransactionState.NONE) {
            throw new IllegalStateException("There is no transaction started");
        }
        if (!messageSentInCurrentTransaction) {
            cleanupTransaction(TransactionState.COMMITTED);
            return Mono.create(MonoSink::success);
        } else {
            ReadConcern readConcern = transactionOptions.getReadConcern();
            if (readConcern == null) {
                throw new MongoInternalException("Invariant violated. Transaction options read concern can not be null");
            }
            boolean alreadyCommitted = commitInProgress || transactionState == TransactionState.COMMITTED;
            commitInProgress = true;
            return Mono.from(sink ->
                    executor.execute(
                            new CommitTransactionOperation(transactionOptions.getWriteConcern(), alreadyCommitted)
                            .recoveryToken(getRecoveryToken())
                            .maxCommitTime(transactionOptions.getMaxCommitTime(MILLISECONDS), MILLISECONDS),
                    readConcern,
                    this.getWrapped(),
                    (result, t) -> {
                        commitInProgress = false;
                        transactionState = TransactionState.COMMITTED;
                        if (t != null) {
                            if (t instanceof MongoException) {
                                unpinServerAddressOnError((MongoException) t);
                            }
                            sink.onError(t);
                        } else {
                            sink.onComplete();
                        }
                    }));
        }
    }

    @Override
    public Publisher<Void> abortTransaction() {
        if (transactionState == TransactionState.ABORTED) {
            throw new IllegalStateException("Cannot call abortTransaction twice");
        }
        if (transactionState == TransactionState.COMMITTED) {
            throw new IllegalStateException("Cannot call abortTransaction after calling commitTransaction");
        }
        if (transactionState == TransactionState.NONE) {
            throw new IllegalStateException("There is no transaction started");
        }
        if (!messageSentInCurrentTransaction) {
            cleanupTransaction(TransactionState.ABORTED);
            return Mono.create(MonoSink::success);
        } else {
            ReadConcern readConcern = transactionOptions.getReadConcern();
            if (readConcern == null) {
                throw new MongoInternalException("Invariant violated. Transaction options read concern can not be null");
            }
            return Mono.from(sink ->
                executor.execute(
                        new AbortTransactionOperation(transactionOptions.getWriteConcern())
                                .recoveryToken(getRecoveryToken()),
                        readConcern,
                        this.getWrapped(),
                        (result, t) -> {
                            if (t instanceof MongoException) {
                                unpinServerAddressOnError((MongoException) t);
                            }
                            cleanupTransaction(TransactionState.ABORTED);
                            sink.onComplete();
                        })
            );
        }
    }

    private void unpinServerAddressOnError(final MongoException e) {
        if (e.hasErrorLabel(TRANSIENT_TRANSACTION_ERROR_LABEL) || e.hasErrorLabel(UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)) {
            setPinnedServerAddress(null);
        }
    }

    // TODO: should there be a version of this that takes a callback?
    @Override
    public void close() {
        if (transactionState == TransactionState.IN) {
            Mono.from(abortTransaction()).doOnSuccess(it -> close()).subscribe();
        } else {
            super.close();
        }
    }

    private void cleanupTransaction(final TransactionState nextState) {
        messageSentInCurrentTransaction = false;
        transactionOptions = null;
        transactionState = nextState;
    }
}
