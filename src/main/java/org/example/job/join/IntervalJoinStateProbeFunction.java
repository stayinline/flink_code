package org.example.job.join;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.example.dto.EducationClickEvent;
import org.example.dto.EducationExposureEvent;

import java.util.ArrayList;
import java.util.List;

public class IntervalJoinStateProbeFunction
        extends CoProcessFunction<EducationExposureEvent, EducationClickEvent, String> {

    private final long lowerBoundMs;
    private final long upperBoundMs;

    private transient MapState<Long, Integer> exposureBuffer;
    private transient MapState<Long, Integer> clickBuffer;
    private transient MapState<Long, List<Long>> exposureCleanupIndex;
    private transient MapState<Long, List<Long>> clickCleanupIndex;

    public IntervalJoinStateProbeFunction(long lowerBoundMs, long upperBoundMs) {
        this.lowerBoundMs = lowerBoundMs;
        this.upperBoundMs = upperBoundMs;
    }

    @Override
    public void open(Configuration parameters) {
        exposureBuffer =
                getRuntimeContext().getMapState(
                        new MapStateDescriptor<>(
                                "probe-exposure-buffer",
                                LongSerializer.INSTANCE,
                                IntSerializer.INSTANCE));
        clickBuffer =
                getRuntimeContext().getMapState(
                        new MapStateDescriptor<>(
                                "probe-click-buffer",
                                LongSerializer.INSTANCE,
                                IntSerializer.INSTANCE));
        exposureCleanupIndex =
                getRuntimeContext().getMapState(
                        new MapStateDescriptor<>(
                                "probe-exposure-cleanup-index",
                                LongSerializer.INSTANCE,
                                new ListSerializer<>(LongSerializer.INSTANCE)));
        clickCleanupIndex =
                getRuntimeContext().getMapState(
                        new MapStateDescriptor<>(
                                "probe-click-cleanup-index",
                                LongSerializer.INSTANCE,
                                new ListSerializer<>(LongSerializer.INSTANCE)));
    }

    @Override
    public void processElement1(
            EducationExposureEvent exposure,
            Context ctx,
            Collector<String> out) throws Exception {
        long cleanupTs = leftCleanupTimestamp(exposure.getTs(), upperBoundMs);
        increment(exposureBuffer, exposure.getTs());
        addCleanup(exposureCleanupIndex, cleanupTs, exposure.getTs());
        ctx.timerService().registerEventTimeTimer(cleanupTs);
        out.collect(formatState(
                exposure.getStudentId(),
                "add exposure " + exposure.getExposureId(),
                ctx.timerService().currentWatermark(),
                cleanupTs));
    }

    @Override
    public void processElement2(
            EducationClickEvent click,
            Context ctx,
            Collector<String> out) throws Exception {
        long cleanupTs = rightCleanupTimestamp(click.getTs(), lowerBoundMs);
        increment(clickBuffer, click.getTs());
        addCleanup(clickCleanupIndex, cleanupTs, click.getTs());
        ctx.timerService().registerEventTimeTimer(cleanupTs);
        out.collect(formatState(
                click.getStudentId(),
                "add click " + click.getClickId(),
                ctx.timerService().currentWatermark(),
                cleanupTs));
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        List<Long> exposureTimestamps = exposureCleanupIndex.get(timestamp);
        if (exposureTimestamps != null) {
            for (Long eventTs : exposureTimestamps) {
                exposureBuffer.remove(eventTs);
            }
            exposureCleanupIndex.remove(timestamp);
        }

        List<Long> clickTimestamps = clickCleanupIndex.get(timestamp);
        if (clickTimestamps != null) {
            for (Long eventTs : clickTimestamps) {
                clickBuffer.remove(eventTs);
            }
            clickCleanupIndex.remove(timestamp);
        }

        out.collect(formatState(
                "-",
                "cleanup timer fired",
                ctx.timerService().currentWatermark(),
                timestamp));
    }

    static long leftCleanupTimestamp(long exposureTs, long upperBoundMs) {
        return upperBoundMs > 0L ? exposureTs + upperBoundMs : exposureTs;
    }

    static long rightCleanupTimestamp(long clickTs, long lowerBoundMs) {
        return lowerBoundMs <= 0L ? clickTs : clickTs - lowerBoundMs;
    }

    private static void increment(MapState<Long, Integer> state, long ts) throws Exception {
        Integer current = state.get(ts);
        state.put(ts, current == null ? 1 : current + 1);
    }

    private static void addCleanup(
            MapState<Long, List<Long>> cleanupIndex,
            long cleanupTs,
            long eventTs) throws Exception {
        List<Long> values = cleanupIndex.get(cleanupTs);
        if (values == null) {
            values = new ArrayList<>();
        }
        values.add(eventTs);
        cleanupIndex.put(cleanupTs, values);
    }

    private String formatState(String key, String action, long watermark, long timerTs) throws Exception {
        return String.format(
                "[INTERVAL_STATE_PROBE] key=%s action=%s watermark=%s timerTs=%d "
                        + "retainedExposures=%d retainedClicks=%d bounds=[+%dms,+%dms]",
                key,
                action,
                watermark == Long.MIN_VALUE ? "MIN_VALUE" : String.valueOf(watermark),
                timerTs,
                count(exposureBuffer),
                count(clickBuffer),
                lowerBoundMs,
                upperBoundMs);
    }

    private static long count(MapState<Long, Integer> state) throws Exception {
        long total = 0;
        Iterable<Integer> values = state.values();
        if (values != null) {
            for (Integer value : values) {
                if (value != null) {
                    total += value;
                }
            }
        }
        return total;
    }
}
