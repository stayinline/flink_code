package org.example.job.trigger;

import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * 自定义 Trigger：窗口内每攒满 {@code countThreshold} 条元素提前 {@link TriggerResult#FIRE}（不 PURGE），
 * 并在事件时间到达窗口结束时再次 FIRE（与 {@code EventTimeTrigger} 默认行为一致）。
 * <p>
 * 对比默认 {@code EventTimeTrigger}：后者仅在 {@code onEventTime(window.maxTimestamp())} 时 FIRE 一次。
 */
public class CountOrTimeTrigger<W extends Window> extends Trigger<Object, W> {

    private static final long serialVersionUID = 1L;

    private final long countThreshold;
    private final ReducingStateDescriptor<Long> countStateDesc;

    private CountOrTimeTrigger(long countThreshold) {
        this.countThreshold = countThreshold;
        this.countStateDesc = new ReducingStateDescriptor<>(
                "count-or-time-trigger-count",
                Long::sum,
                LongSerializer.INSTANCE
        );
    }

    public static <W extends Window> CountOrTimeTrigger<W> of(long countThreshold) {
        return new CountOrTimeTrigger<>(countThreshold);
    }

    @Override
    public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws Exception {
        ReducingState<Long> count = ctx.getPartitionedState(countStateDesc);
        count.add(1L);
        long current = count.get();

        // 注册窗口结束定时器（与 EventTimeTrigger 相同）
        ctx.registerEventTimeTimer(window.maxTimestamp());

        if (current % countThreshold == 0) {
            // 提前输出：FIRE 但不 PURGE，窗口内元素保留供后续累加与最终触发
            return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, W window, TriggerContext ctx) {
        if (time == window.maxTimestamp()) {
            return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(W window, TriggerContext ctx) throws Exception {
        ctx.deleteEventTimeTimer(window.maxTimestamp());
        ctx.getPartitionedState(countStateDesc).clear();
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(W window, OnMergeContext ctx) throws Exception {
        ctx.mergePartitionedState(countStateDesc);
        long windowMaxTimestamp = window.maxTimestamp();
        if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
            ctx.registerEventTimeTimer(windowMaxTimestamp);
        }
    }

    public long getCountThreshold() {
        return countThreshold;
    }
}
