package org.example.job.sql;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.dto.CourseDimRecord;

/**
 * 注册课程维表临时视图（模拟 JDBC/Redis Lookup 源）。
 */
public final class CourseDimRegistrar {

    private CourseDimRegistrar() {
    }

    public static void registerCourseDim(StreamTableEnvironment tEnv, StreamExecutionEnvironment env) {
        DataStream<CourseDimRecord> dimStream = env.fromCollection(CourseDimStore.allRecords());

        Schema dimSchema = Schema.newBuilder()
                .column("course_id", DataTypes.STRING().notNull())
                .column("course_name", DataTypes.STRING())
                .column("category", DataTypes.STRING())
                .columnByExpression("proctime", "PROCTIME()")
                .build();

        tEnv.createTemporaryView(
                "course_dim",
                tEnv.fromDataStream(dimStream, dimSchema));
        System.out.println("[SQL-DIM] course_dim 已注册 " + CourseDimStore.allRecords().size() + " 条（FOR SYSTEM_TIME AS OF）");
    }
}
