# Flink Database Sync Job

This project provides a lightweight, configurable Flink job for efficiently synchronizing data between databases using Flink CDC (Change Data Capture).

## Key Features

*   **Configuration-Driven:** Job behavior is controlled by an external `config.properties` file.
*   **Flexible Sync Modes:** Supports two modes via the `source.startup.mode` property:
    *   `initial`: Performs a one-time, full snapshot of all tables. Ideal for data migration.
    *   `latest` (Default): Performs a full snapshot, then seamlessly switches to capturing real-time incremental changes (inserts, updates, deletes). Ideal for continuous data replication.
*   **Lightweight:** No external message queue (like Kafka) is required, simplifying the architecture.

---

## Usage Scenarios

### Scenario 1: One-Time Data Migration

If you only need to copy the current state of a database to another location (e.g., for testing, archiving, or a one-off migration), use the `initial` mode.

1.  Set `source.startup.mode=initial` in your `config.properties`.
2.  Run the job.
3.  The job will perform a full snapshot of all configured tables and will **automatically stop** once completed.

### Scenario 2: Continuous Data Replication

If you need to keep a target database continuously in sync with a source database (e.g., for building a read replica, feeding a data warehouse, or for high availability), use the `latest` mode.

1.  Set `source.startup.mode=latest` in your `config.properties`.
2.  Run the job.
3.  The job will first perform a full snapshot to ensure data consistency. After the snapshot is complete, it will **automatically switch** to reading the binary log (binlog) to capture and apply all subsequent changes in real-time. The job will run indefinitely until manually stopped.

---

## ⚠️ Important Compatibility Notice

**This project is currently NOT compatible with MySQL Server version 8.4.0 or newer.**

*   **Reason:** MySQL 8.4.0 removed the `SHOW MASTER STATUS` command, which is a core command used by the current and all past versions of the Flink CDC MySQL connector to read the binary log (binlog).
*   **Impact:** Attempting to run this job against a MySQL 8.4+ source will result in a `java.sql.SQLSyntaxErrorException`.
*   **Recommendation:** Please use a MySQL 8.0.x version for the source database until the Flink CDC community releases a new connector version that officially supports MySQL 8.4+.

---

## How to Use

1.  **Configure:** Copy the `database-sync/src/main/resources/config.properties.template` file to `config.properties`.
    *   Fill in the connection details for your source and target databases.
    *   Set the desired `source.startup.mode` (`initial` or `latest`) based on your scenario.

2.  **Build:** Build the project using Maven:
    ```bash
    mvn clean package
    ```

3.  **Run:** Submit the Flink job to your cluster or run it locally.
