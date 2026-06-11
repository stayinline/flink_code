package org.example.job.sql;

/**
 * Flink SQL 语句集中定义，便于 Job 执行与单测引用。
 */
public final class SqlDemoStatements {

    private SqlDemoStatements() {
    }

    public static String createStudySourceDdl(String broker, String topic, String groupId) {
        return ""
                + "CREATE TABLE study_source ("
                + "  eventId STRING,"
                + "  studentId STRING,"
                + "  courseId STRING,"
                + "  eventType STRING,"
                + "  watchSec INT,"
                + "  ts BIGINT,"
                + "  tag STRING,"
                + "  event_time AS TO_TIMESTAMP_LTZ(ts, 3),"
                + "  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND,"
                + "  proctime AS PROCTIME()"
                + ") WITH ("
                + "  'connector' = 'kafka',"
                + "  'topic' = '" + topic + "',"
                + "  'properties.bootstrap.servers' = '" + broker + "',"
                + "  'properties.group.id' = '" + groupId + "',"
                + "  'scan.startup.mode' = 'latest-offset',"
                + "  'format' = 'json',"
                + "  'json.ignore-parse-errors' = 'true'"
                + ")";
    }

    public static String createPrintSinkDdl() {
        return ""
                + "CREATE TABLE sql_print_sink ("
                + "  output_type STRING,"
                + "  student_id STRING,"
                + "  course_id STRING,"
                + "  course_name STRING,"
                + "  category STRING,"
                + "  watch_sec INT,"
                + "  total_watch BIGINT,"
                + "  event_cnt BIGINT,"
                + "  window_start TIMESTAMP(3),"
                + "  window_end TIMESTAMP(3),"
                + "  tag STRING"
                + ") WITH ('connector' = 'print')";
    }

    public static String tumbleWindowAgg(int windowSec) {
        return ""
                + "SELECT"
                + "  'WINDOW' AS output_type,"
                + "  CAST(NULL AS STRING) AS student_id,"
                + "  courseId AS course_id,"
                + "  CAST(NULL AS STRING) AS course_name,"
                + "  CAST(NULL AS STRING) AS category,"
                + "  CAST(NULL AS INT) AS watch_sec,"
                + "  CAST(SUM(watchSec) AS BIGINT) AS total_watch,"
                + "  CAST(COUNT(*) AS BIGINT) AS event_cnt,"
                + "  TUMBLE_START(event_time, INTERVAL '" + windowSec + "' SECOND) AS window_start,"
                + "  TUMBLE_END(event_time, INTERVAL '" + windowSec + "' SECOND) AS window_end,"
                + "  CAST(NULL AS STRING) AS tag"
                + " FROM study_source"
                + " WHERE eventType = 'video_progress'"
                + " GROUP BY courseId, TUMBLE(event_time, INTERVAL '" + windowSec + "' SECOND)";
    }

    public static String temporalLookupJoin() {
        return ""
                + "SELECT"
                + "  'LOOKUP' AS output_type,"
                + "  s.studentId AS student_id,"
                + "  s.courseId AS course_id,"
                + "  d.course_name,"
                + "  d.category,"
                + "  s.watchSec AS watch_sec,"
                + "  CAST(NULL AS BIGINT) AS total_watch,"
                + "  CAST(NULL AS BIGINT) AS event_cnt,"
                + "  CAST(NULL AS TIMESTAMP(3)) AS window_start,"
                + "  CAST(NULL AS TIMESTAMP(3)) AS window_end,"
                + "  s.tag"
                + " FROM study_source AS s"
                + " LEFT JOIN course_dim FOR SYSTEM_TIME AS OF s.proctime AS d"
                + " ON s.courseId = d.course_id"
                + " WHERE s.eventType = 'video_progress'";
    }

    public static String lookupThenTumbleWindow(int windowSec) {
        return ""
                + "SELECT"
                + "  'FULL' AS output_type,"
                + "  CAST(NULL AS STRING) AS student_id,"
                + "  CAST(NULL AS STRING) AS course_id,"
                + "  CAST(NULL AS STRING) AS course_name,"
                + "  e.category,"
                + "  CAST(NULL AS INT) AS watch_sec,"
                + "  CAST(SUM(e.watchSec) AS BIGINT) AS total_watch,"
                + "  CAST(COUNT(*) AS BIGINT) AS event_cnt,"
                + "  TUMBLE_START(e.event_time, INTERVAL '" + windowSec + "' SECOND) AS window_start,"
                + "  TUMBLE_END(e.event_time, INTERVAL '" + windowSec + "' SECOND) AS window_end,"
                + "  CAST(NULL AS STRING) AS tag"
                + " FROM ("
                + "   SELECT s.*, d.category, d.course_name"
                + "   FROM study_source AS s"
                + "   LEFT JOIN course_dim FOR SYSTEM_TIME AS OF s.proctime AS d"
                + "   ON s.courseId = d.course_id"
                + "   WHERE s.eventType = 'video_progress'"
                + " ) AS e"
                + " GROUP BY e.category, TUMBLE(e.event_time, INTERVAL '" + windowSec + "' SECOND)";
    }

    public static String resolveProcessSql(SqlDemoConfigurator.SqlDemoOptions options) {
        switch (options.scenario) {
            case SqlDemoConfigurator.SCENARIO_WINDOW:
                return tumbleWindowAgg(options.windowSec);
            case SqlDemoConfigurator.SCENARIO_LOOKUP:
                return temporalLookupJoin();
            case SqlDemoConfigurator.SCENARIO_FULL:
            default:
                return lookupThenTumbleWindow(options.windowSec);
        }
    }
}
