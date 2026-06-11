-- Flink CDC 增量快照 Demo 初始化脚本
-- 执行：mysql -h 192.168.1.124 -u root -p < init_mysql.sql

CREATE DATABASE IF NOT EXISTS flink_cdc_demo
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

USE flink_cdc_demo;

-- 在线教育：学员选课表（必须有主键，供 chunk 切分）
CREATE TABLE IF NOT EXISTS student_enrollment (
    id              BIGINT       NOT NULL AUTO_INCREMENT COMMENT '主键，chunk 切分键',
    student_id      VARCHAR(32)  NOT NULL COMMENT '学员 ID',
    course_id       VARCHAR(32)  NOT NULL COMMENT '课程 ID',
    enroll_status   VARCHAR(16)  NOT NULL DEFAULT 'enrolled' COMMENT 'enrolled/dropped/completed',
    updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY idx_student (student_id),
    KEY idx_course (course_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='CDC Demo 选课表';

-- MySQL binlog 前置（需 DBA 确认）
-- log_bin=ON, binlog_format=ROW, binlog_row_image=FULL
-- server_id 唯一；CDC 用户需 REPLICATION SLAVE, REPLICATION CLIENT, SELECT

-- 清空演示数据（可选）
-- TRUNCATE TABLE student_enrollment;

-- 预置 3 条（initial 模式启动 Job 时会进入 snapshot 阶段读取）
INSERT INTO student_enrollment (student_id, course_id, enroll_status) VALUES
('S90001', 'C_JAVA',   'enrolled'),
('S90002', 'C_PYTHON', 'enrolled'),
('S90003', 'C_MATH',   'completed')
ON DUPLICATE KEY UPDATE enroll_status = VALUES(enroll_status);
