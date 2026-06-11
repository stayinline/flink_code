package org.example.job.savepoint;

/**
 * 发布/恢复时算子 UID 必须稳定；变更 UID 会被视为新算子，旧状态无法挂载。
 * <p>
 * 生产规范：所有有状态算子在首次上线前显式 {@code .uid(...)}，后续版本禁止随意修改。
 */
public final class SavepointOperatorUids {

    /** Kafka Source（FlinkKafkaConsumer 自带 offset Operator State） */
    public static final String KAFKA_SOURCE = "sp-demo-kafka-source";

    /** JSON 解析 + 过滤（无状态，但建议固定 UID 便于拓扑对比） */
    public static final String PARSE_FILTER = "sp-demo-parse-filter";

    /** 学分/观看时长累计（Keyed State：MapState） */
    public static final String COURSE_CREDIT_ACCUMULATOR = "sp-demo-course-credit-accumulator";

    /** 演示用：错误 UID（故意与线上一致版本不同，用于复现恢复失败） */
    public static final String COURSE_CREDIT_ACCUMULATOR_BROKEN = "sp-demo-course-credit-accumulator-v2-wrong";

    private SavepointOperatorUids() {
    }
}
