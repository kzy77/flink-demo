# Decision Log

This file records architectural and implementation decisions using a list format.
2025-09-05 20:35:53 - Log of updates made.

*

## Decision
*   [2025-09-05 20:38:18] - **采用 Flink JDBC Connector:**
    *   **Rationale:** Flink JDBC Connector 是官方支持的连接器，提供了强大的功能，如并行读取、写入时自动重试以及对多种数据库的广泛支持。这符合我们轻量级和可扩展的设计目标。
    *   **Implementation Details:** 将在 `pom.xml` 中添加 `flink-connector-jdbc` 依赖。作业将通过编程方式配置 JDBC Source 和 Sink，而不是使用 SQL DDL，以实现更大的灵活性。

*

## Rationale 

*

## Implementation Details

*
*   [2025-09-05 20:38:32] - **分离配置与代码:**
    *   **Rationale:** 将所有环境特定的配置（如数据库 URL、用户名、密码）保存在 `.properties` 文件中，可以使代码更具可移植性，并简化不同环境（开发、测试、生产）的部署。通过命令行参数覆盖配置提供了额外的灵活性。
    *   **Implementation Details:** 将创建一个 `ParameterTool` 实例来加载配置文件和解析命令行参数。此 `ParameterTool` 将用于在整个作业中访问配置值。

---
### Decision (Code)
[2025-09-05 21:05:00] - **采用 Flink CDC 替代 JDBC Connector 进行整库同步**

**Rationale:**
用户明确提出需要整库同步能力。相比于通过 JDBC 元数据扫描并对每张表进行全量 `SELECT` 的批处理方式，Flink CDC 提供了“一次性快照 + 增量日志读取”的流式处理方案。这不仅更高效，对源数据库压力更小，还能实现持续的低延迟数据同步，并且支持 Schema 演进，完全符合项目的核心需求。这是一个更优、更现代的解决方案。

**Details:**
- 将从 `pom.xml` 中移除 `flink-connector-jdbc`。
- 将添加 `flink-connector-mysql-cdc` 作为核心依赖。
- `DatabaseSyncJob.java` 的实现将围绕 `MySqlSource` 进行重构，不再需要手动进行元数据发现和表轮询。


---
### Decision (Code)
[2025-09-05 21:10:00] - **迁移到 Apache Flink 官方 CDC Connector**

**Rationale:**
用户指出 Flink CDC 已捐赠给 Apache Flink 社区。使用官方的 `org.apache.flink:flink-connector-mysql-cdc` 依赖是正确的、面向未来的做法，可以确保我们获得社区的持续支持和更新。旧的 `com.ververica` 依赖已不再是推荐版本。

**Details:**
- `pom.xml` 中的 CDC 依赖将从 `com.ververica:flink-connector-mysql-cdc` 更新为 `org.apache.flink:flink-connector-mysql-cdc`。
- `DatabaseSyncJob.java` 中的所有 `import` 语句将从 `com.ververica.cdc...

---
### Decision (Code)
[2025-09-05 21:28:00] - **修正 Flink CDC 版本以确保兼容性**

**Rationale:**
用户指出可能存在 JAR 冲突。经过复核，发现之前为 Flink `1.19.1` 选择的 CDC 连接器版本 (`3.2.0`) 过于超前，可能导致不兼容。为解决潜在的 JAR 冲突并确保稳定性，将 CDC 连接器版本降级至与 Flink `1.19.x` 系列官方适配的 `3.1.0` 版本。

**Details:**
- `pom.xml` 中的 `mysql-cdc.version` 属性已从 `3.2.0` 更改为 `3.1.0`。

---
### Decision (Code)
[2025-09-05 21:33:00] - **在 POM 中添加 HTTPS 仓库以解决 Maven 构建错误**

**Rationale:**
用户反馈 Maven 构建失败，错误信息显示 Maven 3.8.1+ 版本默认阻止了通过不安全的 HTTP 协议访问仓库。为解决此环境配置问题，在项目的 `pom.xml` 文件中明确添加了一个使用 HTTPS 协议的阿里云镜像仓库。这会覆盖全局配置中的不安全 HTTP 地址，确保项目本身的可移植性和构建成功率。

**Details:**
- 在 `pom.xml` 的顶层添加了 `<repositories>` 部分，并指向 `https://maven.aliyun.com/nexus/content/groups/public/`。

---
### Decision (Code)
[2025-09-05 21:41:00] - **在 POM 中添加 Maven Central 仓库以解决 BOM 解析失败问题**

**Rationale:**
用户反馈 `flink-bom` 无法从阿里云镜像解析。为彻底解决此问题并增强构建的稳定性，在 `pom.xml` 的 `<repositories>` 部分添加了官方的 Maven Central 仓库 (`https://repo.maven.apache.org/maven2/`) 作为阿里云镜像的回退。这确保了即使镜像同步延迟或不完整，核心构件也能被成功下载。

**Details:**
- 在 `pom.xml` 的 `<repositories>` 部分，将 Maven Central 添加为第一个仓库。

---
### Decision (Code)
[2025-09-05 21:45:00] - **移除不存在的 Flink BOM 并修正依赖版本**

**Rationale:**
用户指出 `flink-bom` 构件不存在，导致构建失败。经过确认，这是一个严重的配置错误。正确的做法是在每个 Flink 依赖中直接声明版本。为修正此问题，已从 `<dependencyManagement>` 中移除对 `flink-bom` 的导入，并在 `flink-java` 和 `flink-streaming-java` 中恢复了版本声明。

**Details:**
- 从 `pom.xml` 的 `<dependencyManagement>` 部分移除了 `flink-bom`。
- 为 `flink-java` 和 `flink-streaming-java` 添加了 `<version>${flink.version}</version>`。

---
### Decision (Code)
[2025-09-05 21:47:00] - **将核心 Flink 依赖范围设置为 'provided' 以解决运行时冲突**

**Rationale:**
用户反馈版本冲突问题依然存在。根本原因在于核心 Flink 依赖（如 `flink-java`, `flink-streaming-java`）被错误地打包进了作业 JAR 中，这会导致与 Flink 集群自带的库产生冲突。为遵循 Flink 最佳实践并彻底解决此问题，已将这些核心依赖的 Maven scope 更改为 `provided`。这确保它们仅用于编译，而不会被打包，从而避免了运行时的 JAR 冲突。

**Details:**
- 在 `pom.xml` 中，为 `flink-java` 和 `flink-streaming-java` 依赖添加了 `<scope>provided</scope>`。

---
### Decision (Code)
[2025-09-05 21:55:00] - **显式添加 'flink-connector-base' 依赖以解决 NoClassDefFoundError**

**Rationale:**
用户反馈运行时出现 `NoClassDefFoundError: org/apache/flink/connector/base/source/reader/RecordEmitter`。根本原因在于 CDC 连接器所依赖的核心类 `RecordEmitter` 未被正确加载。为解决此问题，在 `pom.xml` 中显式添加了 `flink-connector-base` 依赖，并将其 scope 设置为 `provided`，以确保该类在编译和运行时都可用，同时遵循 Flink 的打包规范。

**Details:**
- 在 `pom.xml` 中，添加了对 `org.apache.flink:flink-connector-base` 的依赖，并设置了 `<scope>provided</scope>`。

---
### Decision (Code)
[2025-09-05 21:57:00] - **从 POM 中移除仓库配置以保持环境无关性**

**Rationale:**
用户指出不应在项目 `pom.xml` 中定义仓库。这是正确的 Maven 最佳实践，项目本身应保持环境无关，而仓库镜像等配置应由构建环境（如 `settings.xml`）负责。为遵循此原则，已从 `pom.xml` 中移除了 `<repositories>` 部分。

**Details:**
- 从 `pom.xml` 中删除了整个 `<repositories>` 块。

---
### Decision (Code)
[2025-09-05 21:58:00] - **添加 'flink-clients' 依赖以支持 Flink 1.18+ 本地执行**

**Rationale:**
用户反馈运行时出现 `IllegalStateException: No ExecutorFactory found`。这是 Flink 1.18+ 版本的已知变化，即在 IDE 中本地执行时需要显式提供执行器依赖。为解决此问题，在 `pom.xml` 中添加了 `flink-clients` 依赖，并将其 scope 设置为 `provided`。这确保了本地执行环境的完整性，同时不会将不必要的依赖打包到生产 JAR 中。

**Details:**
- 在 `pom.xml` 中，添加了对 `org.apache.flink:flink-clients` 的依赖，并设置了 `<scope>provided</scope>`。

---
### Decision (Code)
[2025-09-05 22:00:00] - **添加 Flink Table API 依赖以解决 CDC 内部依赖问题**

**Rationale:**
用户反馈运行时出现 `NoClassDefFoundError: org/apache/flink/table/catalog/ObjectPath`。根本原因在于 Flink CDC 连接器内部实现依赖了 Flink Table API 的组件，而这些组件并未在项目中声明。为解决此问题，在 `pom.xml` 中显式添加了 `flink-table-api-java-bridge` 和 `flink-table-runtime` 依赖，并将其 scope 设置为 `provided`，以确保运行时类路径的完整性。

**Details:**
- 在 `pom.xml` 中，添加了对 `flink-table-api-java-bridge` 和 `flink-table-runtime` 的依赖，并设置了 `<scope>provided</scope>`。

---
### Decision
[2025-09-05 23:11:20] - **确定采用无Kafka的端到端同步架构**

**Rationale:**
根据用户请求，为了降低系统复杂性、运维成本和端到端延迟，决定不引入Kafka作为中间消息队列。采用Flink CDC Source直接连接Flink JDBC Sink的模式，构建一个从源数据库到目标数据库的、轻量级的、端到端的流式同步管道。

**Implications/Details:**
- 整体架构将由`Flink CDC Source -> Flink JDBC Sink`组成。
- 作业将保持高度可配置性，通过`.properties`文件进行驱动。
- 无需部署和维护Kafka集群，简化了整体技术栈。
- `DatabaseSyncJob.java`的实现将聚焦于组装这个简化的数据流。

---
### Decision (Code/DevOps)
[2025-09-06 12:37:23] - Finalized MySQL permissions for Flink CDC source connector.

**Rationale:**
The Flink CDC connector for MySQL requires a specific set of GLOBAL privileges to perform its functions. Initial attempts with minimal or database-specific permissions (`ON `database`.*`) failed because operations like `RELOAD` (for consistent snapshots) and binlog position discovery (`SHOW MASTER STATUS`) require system-wide permissions. The final working configuration requires global (`ON *.*`) grants for `SELECT`, `RELOAD`, `SHOW DATABASES`, `REPLICATION SLAVE`, and `REPLICATION CLIENT`. This allows the connector to read all necessary tables for the snapshot, lock them for consistency, discover databases, and correctly identify the binlog position for streaming changes.

**Details:**
The following SQL command provides the necessary privileges for a user named `binlog_src`:
```sql
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'binlog_src'@'%';
FLUSH PRIVILEGES;
```