package org.example.job.stability;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.dto.CourseWatchPartial;

/**
 * 两阶段聚合 — Global 阶段：按 courseId 合并各 salt 局部结果。
 */
public class GlobalCourseMergeFunction
        extends KeyedProcessFunction<String, CourseWatchPartial, String> {

    private transient ValueState<Long> globalWatchSec;
    private transient ValueState<Long> globalCount;

    @Override
    public void open(Configuration parameters) {
        globalWatchSec = getRuntimeContext().getState(
                new ValueStateDescriptor<>("global-total-sec", Long.class));
        globalCount = getRuntimeContext().getState(
                new ValueStateDescriptor<>("global-total-count", Long.class));
    }

    @Override
    public void processElement(CourseWatchPartial partial, Context ctx, Collector<String> out)
            throws Exception {
        long total = (globalWatchSec.value() != null ? globalWatchSec.value() : 0L)
                + partial.getPartialWatchSec();
        long count = (globalCount.value() != null ? globalCount.value() : 0L)
                + partial.getPartialCount();
        globalWatchSec.update(total);
        globalCount.update(count);

        out.collect(String.format(
                "[2PHASE-GLOBAL] subtask=%d course=%s total=%ds count=%d mergedSalt=%d tag=%s",
                getRuntimeContext().getIndexOfThisSubtask(),
                ctx.getCurrentKey(),
                total,
                count,
                partial.getSalt(),
                partial.getTag()));
    }
}
