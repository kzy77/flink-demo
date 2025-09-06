package com.github.chunkj.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.InputStream;

public class DatabaseSyncJob {

    public static void main(String[] args) throws Exception {
        // 1. Load properties from config file and command line first
        ParameterTool params = loadProperties(args);

        // 2. Create execution environment based on configuration
        final StreamExecutionEnvironment env;
        boolean enableWebUI = params.getBoolean("flink.local.webui.enabled", false);

        if (enableWebUI) {
            // Start with Web UI for local debugging
            Configuration conf = new Configuration();
            env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        } else {
            // Use standard environment for production/cluster deployment
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        }
        env.setParallelism(1);

        // 3. Determine startup mode from config
        String startupMode = params.get("source.startup.mode", "latest");
        StartupOptions startupOptions;
        switch (startupMode.toLowerCase()) {
            case "initial":
                startupOptions = StartupOptions.initial();
                break;
            case "latest":
            default:
                startupOptions = StartupOptions.latest();
                break;
        }

        // 4. Create MySQL CDC Source
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(params.get("source.hostname"))
                .port(params.getInt("source.port"))
                .databaseList(params.get("source.database"))
                .tableList(params.get("source.database") + ".*") // Monitor all tables in the specified database
                .username(params.get("source.username"))
                .password(params.get("source.password"))
                .serverTimeZone(params.get("source.server-time-zone", "UTC")) // Set the timezone of the source database
                .startupOptions(startupOptions) // Set startup mode dynamically
                .deserializer(new JsonDebeziumDeserializationSchema()) // Converts records to JSON
                .build();

        // 5. Create the custom Dynamic JDBC Sink, now with source DB info
        String sourceJdbcUrl = String.format("jdbc:mysql://%s:%d/%s",
                params.get("source.hostname"),
                params.getInt("source.port"),
                params.get("source.database"));

        boolean autoCreateTable = params.getBoolean("sink.auto-create-tables", true);

        String targetJdbcUrl = String.format("jdbc:mysql://%s:%d/%s",
                params.get("target.hostname"),
                params.getInt("target.port"),
                params.get("target.database"));

        DynamicJdbcSink jdbcSink = new DynamicJdbcSink(
                targetJdbcUrl,
                params.get("target.username"),
                params.get("target.password"),
                sourceJdbcUrl,
                params.get("source.username"),
                params.get("source.password"),
                autoCreateTable
        );

        // 6. Build the data stream: from source to sink
        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL CDC Source")
                .addSink(jdbcSink);

        // 7. Execute the Flink job
        env.execute("MySQL to JDBC Database Sync Job (Mode: " + startupMode + ")");
    }

    private static ParameterTool loadProperties(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        // If a config file path is provided via command line, load it
        if (params.has("config")) {
            try (InputStream in = new java.io.FileInputStream(params.get("config"))) {
                return params.mergeWith(ParameterTool.fromPropertiesFile(in));
            }
        }

        // Otherwise, try to load 'config.properties' from the classpath
        try (InputStream in = DatabaseSyncJob.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (in != null) {
                return params.mergeWith(ParameterTool.fromPropertiesFile(in));
            }
        }

        System.out.println("No 'config.properties' file found in classpath and no --config specified. " +
                "Relying solely on command-line parameters.");
        return params;
    }
}
