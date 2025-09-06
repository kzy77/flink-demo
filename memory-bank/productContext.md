# Product Context

This file provides a high-level overview of the project and the expected product that will be created. Initially it is based upon projectBrief.md (if provided) and all other available project-related information in the working directory. This file is intended to be updated as the project evolves, and should be used to inform all other modes of the project's goals and context.
2025-09-05 20:35:28 - Log of updates made will be appended as footnotes to the end of this file.

*

## Project Goal

*   创建一个轻量级、可配置的 Flink 作业，用于在不同数据库之间进行高效的数据同步。

## Key Features

*   **配置驱动:** 作业行为由外部 `.properties` 文件控制。
*   **灵活的同步模式:** 支持通过 `source.startup.mode` 属性在两种模式间切换：
    *   `initial`: 执行一次性的全量数据同步。
    *   `latest` (默认): 执行全量同步后，继续进行实时的增量同步。
*   **参数化:** 支持通过命令行参数动态传递配置。

## Overall Architecture
*   **端到端流式架构 (无Kafka):**
    *   **配置层:** 通过 `config.properties` 文件和命令行参数驱动作业行为。
    *   **数据源 (Source):** 使用 **Flink CDC Source** 直接从源数据库 (如 MySQL) 的事务日志 (Binlog) 中捕获实时数据变更（包括初始快照和增量更新）。
    *   **数据汇 (Sink):** 使用 **Flink JDBC Sink** 将变更数据以幂等方式（UPSERT）写入目标数据库。
    *   **作业编排:** `DatabaseSyncJob.java` 负责构建从Source到Sink的数据流管道并执行 Flink 作业。

*   