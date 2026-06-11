package org.example.job.join;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.dto.EducationClickEvent;
import org.example.dto.EducationExposureEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 双流 Join 演示数据发送 + Regular/Interval/Window 逻辑单测。
 * <pre>
 * kafka-topics.sh --create --topic test_flink_join_exposure --partitions 2 --bootstrap-server 192.168.1.124:9092
 * kafka-topics.sh --create --topic test_flink_join_click --partitions 2 --bootstrap-server 192.168.1.124:9092
 * </pre>
 */
public class FlinkJoinDemoJobTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    public static final long BASE_TIME_MS = 1_700_800_000_000L;
    public static final String DEMO_STUDENT = "S90001";
    public static final String DEMO_COURSE = "C_LIVE_JAVA";

    @Test
    void sendJoinDemoEvents() throws Exception {
        sendAllScenarios();
    }

    @Test
    void intervalJoin_clickWithin10Min_matches() {
        long expTs = BASE_TIME_MS + 60_000;
        long clickTs = BASE_TIME_MS + 4 * 60_000;
        assertTrue(isIntervalMatch(expTs, clickTs, 10),
                "曝光后 4min 点击应在 10min Interval 内");
        System.out.printf("Interval 匹配: delay=%dms (<%dmin)%n",
                clickTs - expTs, 10);
    }

    @Test
    void intervalJoin_clickAfter10Min_noMatch() {
        long expTs = BASE_TIME_MS;
        long clickTs = BASE_TIME_MS + 11 * 60_000;
        assertFalse(isIntervalMatch(expTs, clickTs, 10),
                "曝光后 11min 点击超出 Interval 上界");
        System.out.println("Interval 不匹配: 11min > upperBound 10min → 状态可清理");
    }

    @Test
    void intervalJoin_clickBeforeExposure_noMatch() {
        long expTs = BASE_TIME_MS + 5 * 60_000;
        long clickTs = BASE_TIME_MS + 60_000;
        assertFalse(isIntervalMatch(expTs, clickTs, 10),
                "lowerBound=0 时 click 早于 exposure 不匹配");
        System.out.println("Interval 边界: click.ts < exposure.ts → 不匹配（业务上点击不能先于曝光）");
    }

    @Test
    void intervalStateProbe_cleanupTimerAtExposurePlusUpperBound() {
        long upperMs = 10 * 60_000L;
        long expTs = BASE_TIME_MS + 30_000;
        long cleanupTs = IntervalJoinStateProbeFunction.leftCleanupTimestamp(expTs, upperMs);
        assertEquals(expTs + upperMs, cleanupTs);
        System.out.printf("状态清理 timer=exposure.ts+upperBound=%d%n", cleanupTs - BASE_TIME_MS);
    }

    @Test
    void windowJoin_same30sWindow_matches() {
        long wMs = 30_000L;
        long windowStart = tumbleWindowStart(BASE_TIME_MS, wMs);
        long expTs = windowStart + 5_000;
        long clickTs = windowStart + 20_000;
        assertTrue(sameTumbleWindow(expTs, clickTs, 30),
                "同 30s Tumbling 窗口内应 Window Join 匹配");
    }

    @Test
    void windowJoin_differentWindow_noMatch() {
        long expTs = BASE_TIME_MS + 5_000;
        long clickTs = BASE_TIME_MS + 40_000;
        assertFalse(sameTumbleWindow(expTs, clickTs, 30),
                "跨 30s 窗口不应 Window Join 匹配");
        System.out.println("Window Join: 仅同窗口；跨窗口 click 丢弃");
    }

    @Test
    void regularJoin_stateGrowsWithoutTtl() {
        RegularJoinStateSimulator sim = new RegularJoinStateSimulator();
        sim.onExposure("REQ-R1");
        sim.onExposure("REQ-R1");
        sim.onClick("REQ-R1");
        assertEquals(2, sim.exposureCount());
        assertEquals(1, sim.clickCount());
        assertEquals(2, sim.matchCount(), "regular: 1 click × 2 exposure = 2 条匹配");
        System.out.printf("Regular Join: retainedExp=%d retainedClick=%d matches=%d ⚠️ 状态增长%n",
                sim.exposureCount(), sim.clickCount(), sim.matchCount());
    }

    @Test
    void threeJoinTypes_comparisonSummary() {
        List<String> rows = List.of(
                "Regular  | 无限保留      | 双流全历史      | 实时+历史回溯 | 必须 State TTL",
                "Interval | exposure+upper| [0,+10min]     | WM 过界清理  | 曝光-点击归因",
                "Window   | 窗口结束      | 同 Tumble 窗   | 窗口触发     | 固定窗口报表"
        );
        assertEquals(3, rows.size());
        rows.forEach(r -> System.out.println("  " + r));
    }

    @Test
    void interviewScript_intervalJoinVsRegular() {
        String answer = "曝光-点击归因只需 10min 内关联，Interval Join 状态按 WM 自动清理；"
                + "Regular Join 保留全历史状态 OOM 风险；Broadcast State 用于维表而非双流事实 Join";
        assertTrue(answer.contains("Interval Join"));
        System.out.println("面试话术: " + answer);
    }

    public static void main(String[] args) throws Exception {
        sendAllScenarios();
    }

    public static void sendAllScenarios() throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, FlinkJoinDemoJob.KAFKA_BROKER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        List<SendPlan> plan = buildPlan();
        printHeader(plan);
        printPlan(plan);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (SendPlan step : plan) {
                if (step.sleepBeforeMs > 0) {
                    System.out.printf("%n--- 等待 %ds：%s ---%n", step.sleepBeforeMs / 1000, step.waitReason);
                    Thread.sleep(step.sleepBeforeMs);
                }
                if (step.exposure != null) {
                    sendExposure(producer, step);
                }
                if (step.click != null) {
                    sendClick(producer, step);
                }
            }
            producer.flush();
        }
        printExpectedOutcomes();
    }

    private static List<SendPlan> buildPlan() {
        List<SendPlan> plan = new ArrayList<>();

        // Phase 1：Interval 正常匹配（曝光 +3min 点击）
        plan.add(exposure("exp01", "REQ001", 0, "interval-ok",
                "Phase1 曝光 +0s"));
        plan.add(click("clk01", "REQ001", 3 * 60, "interval-ok",
                "Phase1 点击 +3min → Interval Join 匹配"));

        // Phase 2：Interval 超时（+12min 点击）
        plan.add(exposure("exp02", "REQ002", 10, "interval-late",
                "Phase2 曝光 +10s"));
        plan.add(click("clk02", "REQ002", 10 + 12 * 60, "interval-late",
                "Phase2 点击 +12min → 超出 10min 上界，不匹配"));

        // Phase 3：Window Join 同窗口（+5s 曝光, +20s 点击）
        plan.add(exposure("exp03", "REQ003", 30, "window-ok",
                "Phase3 曝光 +30s"));
        plan.add(click("clk03", "REQ003", 50, "window-ok",
                "Phase3 点击 +50s 同 30s 窗 → Window Join"));

        // Phase 4：Window Join 跨窗口
        plan.add(exposure("exp04", "REQ004", 60, "window-miss",
                "Phase4 曝光 +60s"));
        plan.add(click("clk04", "REQ004", 100, "window-miss",
                "Phase4 点击 +100s 跨窗 → Window Join 不匹配"));

        // Phase 5：Regular Join 状态膨胀（同 requestId 多次曝光+点击）
        plan.add(exposure("exp05a", "REQ005", 120, "regular-growth",
                "Phase5 Regular: 第1次曝光"));
        plan.add(exposure("exp05b", "REQ005", 125, "regular-growth",
                "Phase5 Regular: 第2次曝光"));
        plan.add(click("clk05", "REQ005", 130, "regular-growth",
                "Phase5 Regular: 1 click 匹配 2 exposure → 2条"));

        // Phase 6：Probe 状态清理（曝光后等待 WM 推进）
        plan.add(exposure("exp06", "REQ006", 150, "probe-cleanup",
                "Phase6 probe: 观察 cleanup timer=exp+10min"));
        plan.add(wait(3000, "等待 WM 推进，probe 模式看 [INTERVAL_STATE_PROBE] cleanup"));

        // Phase 7：flush WM
        plan.add(exposure("exp07", "REQ007", 12 * 60, "flush",
                "Phase7 flush WM 推进清理 exp02/exp06 buffer"));
        plan.add(click("clk07", "REQ001", 12 * 60 + 30, "flush",
                "Phase7 晚到 click（REQ001 旧曝光已清理，Interval 不匹配）"));

        return plan;
    }

    static boolean isIntervalMatch(long exposureTs, long clickTs, int upperMin) {
        long lowerMs = JoinDemoConfigurator.DEFAULT_INTERVAL_LOWER_MS;
        long upperMs = upperMin * 60_000L;
        long delta = clickTs - exposureTs;
        return delta >= lowerMs && delta <= upperMs;
    }

    static boolean sameTumbleWindow(long ts1, long ts2, int windowSec) {
        long wMs = windowSec * 1000L;
        return tumbleWindowStart(ts1, wMs) == tumbleWindowStart(ts2, wMs);
    }

    static long tumbleWindowStart(long ts, long windowMs) {
        return ts - Math.floorMod(ts, windowMs);
    }

    private static SendPlan exposure(String id, String requestId, long offsetSec, String tag, String purpose) {
        return new SendPlan(
                new EducationExposureEvent(id, requestId, DEMO_STUDENT, DEMO_COURSE,
                        BASE_TIME_MS + offsetSec * 1000, "feed_card", tag),
                null, 0, null, purpose);
    }

    private static SendPlan click(String id, String requestId, long offsetSec, String tag, String purpose) {
        return new SendPlan(
                null,
                new EducationClickEvent(id, requestId, DEMO_STUDENT, DEMO_COURSE,
                        BASE_TIME_MS + offsetSec * 1000, "open_course", tag),
                0, null, purpose);
    }

    private static SendPlan wait(long ms, String reason) {
        return new SendPlan(null, null, ms, reason, reason);
    }

    private static void sendExposure(KafkaProducer<String, String> producer, SendPlan step) throws Exception {
        String json = MAPPER.writeValueAsString(step.exposure);
        producer.send(new ProducerRecord<>(
                FlinkJoinDemoJob.TOPIC_EXPOSURE,
                step.exposure.getRequestId(),
                json)).get(10, TimeUnit.SECONDS);
        System.out.printf("[SEND-EXP] id=%s req=%s ts=+%ds tag=%-14s | %s%n",
                step.exposure.getExposureId(), step.exposure.getRequestId(),
                (step.exposure.getTs() - BASE_TIME_MS) / 1000,
                step.exposure.getTag(), step.purpose);
        Thread.sleep(300);
    }

    private static void sendClick(KafkaProducer<String, String> producer, SendPlan step) throws Exception {
        String json = MAPPER.writeValueAsString(step.click);
        producer.send(new ProducerRecord<>(
                FlinkJoinDemoJob.TOPIC_CLICK,
                step.click.getRequestId(),
                json)).get(10, TimeUnit.SECONDS);
        System.out.printf("[SEND-CLK] id=%s req=%s ts=+%ds tag=%-14s | %s%n",
                step.click.getClickId(), step.click.getRequestId(),
                (step.click.getTs() - BASE_TIME_MS) / 1000,
                step.click.getTag(), step.purpose);
        Thread.sleep(300);
    }

    private static void printHeader(List<SendPlan> plan) {
        System.out.println("========================================");
        System.out.println("双流 Join 演示数据发送");
        System.out.println("EXPOSURE: " + FlinkJoinDemoJob.TOPIC_EXPOSURE);
        System.out.println("CLICK:    " + FlinkJoinDemoJob.TOPIC_CLICK);
        System.out.println("共 " + plan.size() + " 步");
        System.out.println("========================================");
    }

    private static void printPlan(List<SendPlan> plan) {
        System.out.println("--- 发送计划 ---");
        for (SendPlan s : plan) {
            if (s.exposure != null) {
                System.out.printf("  [EXP] %s %s%n", s.exposure.getExposureId(), s.purpose);
            } else if (s.click != null) {
                System.out.printf("  [CLK] %s %s%n", s.click.getClickId(), s.purpose);
            } else if (s.waitReason != null) {
                System.out.printf("  [WAIT %ds] %s%n", s.sleepBeforeMs / 1000, s.waitReason);
            }
        }
        System.out.println("----------------");
    }

    private static void printExpectedOutcomes() {
        System.out.println("========================================");
        System.out.println("预期观察：");
        System.out.println("  interval: REQ001 clk01 匹配 | REQ002 clk02 不匹配（+12min）");
        System.out.println("  window:   REQ003 匹配 | REQ004 跨窗不匹配");
        System.out.println("  regular:  REQ005 1 click × 2 exp → 2 条 [REGULAR-JOIN]");
        System.out.println("  probe:    [INTERVAL_STATE_PROBE] cleanup timer fired → retained=0");
        System.out.println("对比实验：");
        System.out.println("  1) FlinkJoinDemoJob interval 10 30 0");
        System.out.println("  2) FlinkJoinDemoJob regular 10 30 0");
        System.out.println("  3) FlinkJoinDemoJob window 10 30 0");
        System.out.println("  4) FlinkJoinDemoJob probe 10 30 0");
        System.out.println("文档: resources/join/FlinkJoinDemoGuide.md");
        System.out.println("========================================");
    }

    static class RegularJoinStateSimulator {
        private int exposures;
        private int clicks;

        void onExposure(String requestId) {
            exposures++;
        }

        void onClick(String requestId) {
            clicks++;
        }

        int exposureCount() {
            return exposures;
        }

        int clickCount() {
            return clicks;
        }

        int matchCount() {
            return exposures * clicks;
        }
    }

    private static class SendPlan {
        final EducationExposureEvent exposure;
        final EducationClickEvent click;
        final long sleepBeforeMs;
        final String waitReason;
        final String purpose;

        SendPlan(EducationExposureEvent exposure, EducationClickEvent click,
                 long sleepBeforeMs, String waitReason, String purpose) {
            this.exposure = exposure;
            this.click = click;
            this.sleepBeforeMs = sleepBeforeMs;
            this.waitReason = waitReason;
            this.purpose = purpose;
        }
    }
}
