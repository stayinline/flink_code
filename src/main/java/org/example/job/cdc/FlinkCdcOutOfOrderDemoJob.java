package org.example.job.cdc;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.example.dto.CdcChangeEvent;
import org.example.dto.CdcEnrollmentSnapshot;
import org.example.job.kafka.FlinkKafkaConnectorDemoJob;
import org.example.job.state.StateBackendConfigurator;

import java.util.Properties;

/**
 * CDC 乱序治理演示：模拟 Canal/Kafka 或 Flink CDC 下游收到的乱序变更，
 * 按 binlog 位点 / 版本号 last-write-wins。
 * <p>
 * 启动参数：{@code versionStrategy parallelism backend}
 * 例：{@code binlog_pos 2 hashmap}
 * <p>
 * 文档：{@code src/main/resources/cdc/FlinkCdcSelectionAndOutOfOrderGuide.md}
 */
public class FlinkCdcOutOfOrderDemoJob {

    public static void main(String[] args) throws Exception {
        CdcOutOfOrderConfigurator.OutOfOrderOptions options =
                CdcOutOfOrderConfigurator.resolveOptions(args);

        Configuration flinkConfig = StateBackendConfigurator.createFlinkConfiguration(options.backend);
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(options.parallelism, flinkConfig);
        env.setParallelism(options.parallelism);

        CdcOutOfOrderConfigurator.configureEnvironment(env, options);

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", FlinkKafkaConnectorDemoJob.KAFKA_BROKER);
        kafkaProps.setProperty("group.id", CdcOutOfOrderConfigurator.GROUP_ID);

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                CdcOutOfOrderConfigurator.TOPIC,
                new SimpleStringSchema(),
                kafkaProps
        );
        consumer.setStartFromLatest();

        DataStream<CdcChangeEvent> eventStream = env
                .addSource(consumer)
                .name("Kafka-CdcOutOfOrder")
                .map(json -> CdcDebeziumJsonParser.parse(json).orElse(null))
                .name("ParseDebeziumJson")
                .filter(new ValidCdcFilter())
                .name("ValidCdcEvent");

        DataStream<CdcEnrollmentSnapshot> governedStream = eventStream
                .keyBy(CdcChangeEvent::getPrimaryKeyId)
                .process(new CdcLastWriteWinsFunction(options.versionStrategy))
                .name("LastWriteWins");

        governedStream.print("LWW结果");

        printStartupBanner(options);
        env.execute("Flink CDC Out-of-Order Governance - LWW [" + options.versionStrategy + "]");
    }

    private static void printStartupBanner(CdcOutOfOrderConfigurator.OutOfOrderOptions options) {
        System.out.println("========================================");
        System.out.println("CDC 乱序治理 Demo 已启动");
        System.out.println("Kafka: " + FlinkKafkaConnectorDemoJob.KAFKA_BROKER);
        System.out.println("Topic: " + CdcOutOfOrderConfigurator.TOPIC);
        System.out.println("版本策略: " + CdcOutOfOrderConfigurator.describeStrategy(options.versionStrategy));
        System.out.println("链路: Kafka(模拟 CDC) → 解析 → keyBy(pk) → LWW → 下游快照");
        System.out.println("文档: resources/cdc/FlinkCdcSelectionAndOutOfOrderGuide.md");
        System.out.println("请运行 FlinkCdcOutOfOrderDemoJobTest 发送乱序 update");
        System.out.println("对比实验：");
        System.out.println("  1) binlog_pos 2 hashmap      — 推荐");
        System.out.println("  2) debezium_ts_ms 2 hashmap");
        System.out.println("  3) db_updated_at 2 hashmap");
        System.out.println("========================================");
    }

    private static class ValidCdcFilter implements FilterFunction<CdcChangeEvent> {
        @Override
        public boolean filter(CdcChangeEvent event) {
            return event != null && event.getPrimaryKeyId() != null;
        }
    }
}
