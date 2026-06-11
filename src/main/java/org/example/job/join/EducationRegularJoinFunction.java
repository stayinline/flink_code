package org.example.job.join;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.example.dto.EducationClickEvent;
import org.example.dto.EducationExposureEvent;
import org.example.dto.JoinResult;

/**
 * 模拟 Regular Join（Inner）——双流状态无限保留，除非显式配置 State TTL。
 * <p>
 * 每来一条 exposure/click 都与对侧历史全量做匹配 → 状态单调增长，演示 Regular Join 危险。
 */
public class EducationRegularJoinFunction
        extends CoProcessFunction<EducationExposureEvent, EducationClickEvent, JoinResult> {

    private final long stateTtlHours;

    private transient ListState<EducationExposureEvent> exposureHistory;
    private transient ListState<EducationClickEvent> clickHistory;
    private transient int exposureCount;
    private transient int clickCount;

    public EducationRegularJoinFunction(long stateTtlHours) {
        this.stateTtlHours = stateTtlHours;
    }

    @Override
    public void open(Configuration parameters) {
        ListStateDescriptor<EducationExposureEvent> expDesc =
                new ListStateDescriptor<>("regular-exposure-history", EducationExposureEvent.class);
        ListStateDescriptor<EducationClickEvent> clickDesc =
                new ListStateDescriptor<>("regular-click-history", EducationClickEvent.class);

        if (stateTtlHours > 0) {
            StateTtlConfig ttl = StateTtlConfig.newBuilder(Time.hours(stateTtlHours))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();
            expDesc.enableTimeToLive(ttl);
            clickDesc.enableTimeToLive(ttl);
        }

        exposureHistory = getRuntimeContext().getListState(expDesc);
        clickHistory = getRuntimeContext().getListState(clickDesc);
    }

    @Override
    public void processElement1(EducationExposureEvent exposure, Context ctx, Collector<JoinResult> out)
            throws Exception {
        exposureHistory.add(exposure);
        exposureCount++;

        for (EducationClickEvent click : clickHistory.get()) {
            out.collect(JoinResult.matched(
                    "REGULAR_JOIN",
                    exposure,
                    click,
                    "regular: 与对侧全量历史匹配，状态不自动清理"));
        }

        logState("exposure+" + exposure.getExposureId());
    }

    @Override
    public void processElement2(EducationClickEvent click, Context ctx, Collector<JoinResult> out)
            throws Exception {
        clickHistory.add(click);
        clickCount++;

        for (EducationExposureEvent exposure : exposureHistory.get()) {
            out.collect(JoinResult.matched(
                    "REGULAR_JOIN",
                    exposure,
                    click,
                    "regular: 新 click 回溯匹配全部历史 exposure"));
        }

        logState("click+" + click.getClickId());
    }

    private void logState(String action) throws Exception {
        int expSize = 0;
        for (EducationExposureEvent ignored : exposureHistory.get()) {
            expSize++;
        }
        int clickSize = 0;
        for (EducationClickEvent ignored : clickHistory.get()) {
            clickSize++;
        }
        System.out.printf(
                "[REGULAR-JOIN] subtask=%d action=%s retainedExposure=%d retainedClick=%d ttl=%dh ⚠️ 无 TTL 则无限增长%n",
                getRuntimeContext().getIndexOfThisSubtask(),
                action,
                expSize,
                clickSize,
                stateTtlHours);
    }
}
