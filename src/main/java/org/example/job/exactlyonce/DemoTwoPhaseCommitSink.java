package org.example.job.exactlyonce;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.example.dto.StudySummaryRecord;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 模拟 Kafka EXACTLY_ONCE / 事务型 Sink 的 2PC 生命周期。
 * <p>
 * 对应骨架：{@code beginTransaction → invoke → preCommit(随 CK) → commit(notifyCheckpointComplete) → abort}
 * <p>
 * 已提交数据写入 {@link #COMMITTED_STORE}（进程内模拟外部系统），便于 Test 断言。
 */
public class DemoTwoPhaseCommitSink
        extends TwoPhaseCommitSinkFunction<StudySummaryRecord, DemoTwoPhaseCommitSink.TxnContext, Void> {

    /** 模拟 Kafka 已提交分区数据（全局可见，供单测） */
    public static final ConcurrentHashMap<String, StudySummaryRecord> COMMITTED_STORE = new ConcurrentHashMap<>();

    private static final AtomicLong TXN_SEQ = new AtomicLong();

    private final long commitSlowMs;

    public DemoTwoPhaseCommitSink(long commitSlowMs) {
        super(
                TypeExtractor.getForClass(TxnContext.class).createSerializer(new ExecutionConfig()),
                VoidSerializer.INSTANCE);
        this.commitSlowMs = commitSlowMs;
    }

    @Override
    protected TxnContext beginTransaction() throws Exception {
        String txnId = "txn-" + TXN_SEQ.incrementAndGet() + "-sub" + getRuntimeContext().getIndexOfThisSubtask();
        System.out.printf("[2PC-BEGIN] subtask=%d txnId=%s%n",
                getRuntimeContext().getIndexOfThisSubtask(), txnId);
        return new TxnContext(txnId);
    }

    @Override
    protected void invoke(TxnContext transaction, StudySummaryRecord value, Context context) throws Exception {
        transaction.buffer.add(value);
        System.out.printf("[2PC-INVOKE] txnId=%s bufferSize=%d | %s%n",
                transaction.txnId, transaction.buffer.size(), value);
    }

    @Override
    protected void preCommit(TxnContext transaction) throws Exception {
        transaction.preCommitted = true;
        System.out.printf("[2PC-PRE-COMMIT] txnId=%s records=%d（随 Checkpoint 快照，尚未对外可见）%n",
                transaction.txnId, transaction.buffer.size());
    }

    @Override
    protected void commit(TxnContext transaction) {
        if (commitSlowMs > 0) {
            try {
                Thread.sleep(commitSlowMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        for (StudySummaryRecord r : transaction.buffer) {
            COMMITTED_STORE.put(r.getDedupKey(), r);
        }
        System.out.printf("[2PC-COMMIT] txnId=%s → 对外可见 %d 条 | storeSize=%d%n",
                transaction.txnId, transaction.buffer.size(), COMMITTED_STORE.size());
    }

    @Override
    protected void abort(TxnContext transaction) {
        System.out.printf("[2PC-ABORT] txnId=%s 丢弃 %d 条未提交缓冲（CK 失败或新 CK 覆盖）%n",
                transaction.txnId, transaction.buffer.size());
        transaction.buffer.clear();
    }

    @Override
    protected void recoverAndCommit(TxnContext transaction) {
        System.out.printf("[2PC-RECOVER-COMMIT] txnId=%s 恢复后补提交%n", transaction.txnId);
        commit(transaction);
    }

    @Override
    protected void recoverAndAbort(TxnContext transaction) {
        System.out.printf("[2PC-RECOVER-ABORT] txnId=%s 恢复后回滚%n", transaction.txnId);
        abort(transaction);
    }

    static class TxnContext implements Serializable {
        final String txnId;
        final List<StudySummaryRecord> buffer = new ArrayList<>();
        boolean preCommitted;

        TxnContext(String txnId) {
            this.txnId = txnId;
        }
    }
}
