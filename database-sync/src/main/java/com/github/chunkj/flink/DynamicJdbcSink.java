package com.github.chunkj.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class DynamicJdbcSink extends RichSinkFunction<String> {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicJdbcSink.class);

    // Target DB info
    private final String targetJdbcUrl;
    private final String targetUsername;
    private final String targetPassword;

    // Source DB info (for schema discovery fallback)
    private final String sourceJdbcUrl;
    private final String sourceUsername;
    private final String sourcePassword;
    private final boolean autoCreateTable;

    private transient Connection targetConnection;
    private transient Connection sourceConnection; // Connection to source DB for metadata queries
    private transient ObjectMapper objectMapper;
    private transient Set<String> knownTables;
    private transient String targetDatabase;
    private transient Map<String, Map<String, Integer>> tableSchemaCache;

    public DynamicJdbcSink(String targetJdbcUrl, String targetUsername, String targetPassword,
                           String sourceJdbcUrl, String sourceUsername, String sourcePassword,
                           boolean autoCreateTable) {
        this.targetJdbcUrl = targetJdbcUrl;
        this.targetUsername = targetUsername;
        this.targetPassword = targetPassword;
        this.sourceJdbcUrl = sourceJdbcUrl;
        this.sourceUsername = sourceUsername;
        this.sourcePassword = sourcePassword;
        this.autoCreateTable = autoCreateTable;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            targetConnection = DriverManager.getConnection(targetJdbcUrl, targetUsername, targetPassword);
            sourceConnection = DriverManager.getConnection(sourceJdbcUrl, sourceUsername, sourcePassword);
            objectMapper = new ObjectMapper();
            knownTables = new HashSet<>();
            tableSchemaCache = new HashMap<>();
            this.targetDatabase = extractDatabaseName(targetJdbcUrl);
            LOG.info("Successfully connected to target and source databases (MySQL). Target DB identified as: {}", this.targetDatabase);
        } catch (Exception e) {
            LOG.error("Failed to open JDBC connections.", e);
            throw new RuntimeException("Failed to open JDBC connections.", e);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (targetConnection != null && !targetConnection.isClosed()) {
            targetConnection.close();
            LOG.info("Target JDBC connection closed.");
        }
        if (sourceConnection != null && !sourceConnection.isClosed()) {
            sourceConnection.close();
            LOG.info("Source JDBC connection closed.");
        }
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        LOG.debug("Received raw CDC message: {}", value);
        try {
            JsonNode root = objectMapper.readTree(value);
            String op = root.path("op").asText();

            // Snapshot records have op 'r' (read)
            if ("r".equals(op)) {
                op = "c"; // Treat snapshot records as create operations
            }

            if (root.path("source").isMissingNode()) {
                LOG.warn("Skipping message with no source info: {}", value);
                return;
            }
            String sourceDb = root.path("source").path("db").asText();
            String table = root.path("source").path("table").asText();
            String targetTable = "`" + this.targetDatabase + "`.`" + table + "`";
            LOG.debug("Processing operation '{}' for table {}", op, targetTable);

            ensureTableExists(sourceDb, table, root);
            
            List<String> primaryKeys = getPrimaryKeys(sourceDb, table, root);
            LOG.debug("Discovered primary keys for table {}: {}", targetTable, primaryKeys);

            switch (op) {
                case "c": // Create
                case "u": // Update
                    handleUpsert(targetTable, primaryKeys, root.path("after"));
                    break;
                case "d": // Delete
                    handleDelete(targetTable, primaryKeys, root.path("before"));
                    break;
                default:
                    LOG.warn("Unsupported operation type '{}' in message: {}", op, value);
            }
        } catch (Exception e) {
            LOG.error("Error processing CDC message: " + value, e);
            throw e;
        }
    }

    private void ensureTableExists(String sourceDb, String table, JsonNode root) throws SQLException {
        String qualifiedTargetTableName = this.targetDatabase + "." + table;
        if (knownTables.contains(qualifiedTargetTableName)) {
            return;
        }

        DatabaseMetaData metaData = targetConnection.getMetaData();
        // Check for table existence in the *target* database
        try (ResultSet rs = metaData.getTables(this.targetDatabase, null, table, new String[]{"TABLE"})) {
            if (rs.next()) {
                knownTables.add(qualifiedTargetTableName);
                LOG.info("Table {} already exists in target database.", qualifiedTargetTableName);
            } else {
                if (autoCreateTable) {
                    LOG.info("Table {} does not exist. Attempting to create it.", qualifiedTargetTableName);
                    // Generate SQL using source schema but for the target database
                    String createTableSql = generateCreateTableSql(sourceDb, table, root);
                    try (Statement stmt = targetConnection.createStatement()) {
                        stmt.execute(createTableSql);
                        knownTables.add(qualifiedTargetTableName);
                        LOG.info("Successfully created table: {}", qualifiedTargetTableName);
                    }
                } else {
                    String errorMessage = String.format("Target table %s does not exist and auto table creation is disabled.", qualifiedTargetTableName);
                    LOG.error(errorMessage);
                    throw new RuntimeException(errorMessage);
                }
            }
        }
    }
    
    private List<String> getPrimaryKeys(String sourceDb, String table, JsonNode root) throws SQLException {
        // Fallback: Get PK from source database metadata
        LOG.debug("Could not determine primary key from message schema. Querying source database for table {}.{}", sourceDb, table);
        List<String> pkColumns = new ArrayList<>();
        DatabaseMetaData metaData = sourceConnection.getMetaData();
        try (ResultSet rs = metaData.getPrimaryKeys(sourceDb, null, table)) {
            while (rs.next()) {
                pkColumns.add(rs.getString("COLUMN_NAME"));
            }
        }
        return pkColumns;
    }

    private String generateCreateTableSql(String sourceDb, String table, JsonNode root) throws SQLException {
        try {
            // Attempt to generate from schema in message first
            return generateCreateTableSqlFromMessage(sourceDb, table, root);
        } catch (IllegalArgumentException e) {
            LOG.warn("Could not generate CREATE TABLE statement from message schema for table {}.{}. Falling back to source DB metadata. Reason: {}", sourceDb, table, e.getMessage());
            // Fallback to generating from source DB metadata
            return generateCreateTableSqlFromSourceDb(sourceDb, table);
        }
    }

    private String generateCreateTableSqlFromMessage(String sourceDb, String table, JsonNode root) {
        JsonNode schema = root.path("schema");
        if (schema.isMissingNode()) {
            throw new IllegalArgumentException("Schema node is missing.");
        }
        
        JsonNode payloadFields = schema.path("fields");
        JsonNode tableSchema = StreamSupport.stream(payloadFields.spliterator(), false)
            .filter(field -> "after".equals(field.path("field").asText()))
            .findFirst()
            .orElse(null);

        if (tableSchema == null || !tableSchema.has("fields")) {
            throw new IllegalArgumentException("Could not find 'after' field schema in CDC message.");
        }

        StringBuilder sb = new StringBuilder("CREATE TABLE `").append(this.targetDatabase).append("`.`").append(table).append("` (\n");
        List<String> pkColumns = new ArrayList<>(); // Logic to find PKs from schema can be complex; simplified here

        for (JsonNode field : tableSchema.path("fields")) {
            String columnName = field.path("name").asText();
            String columnType = getMysqlType(field.path("type").asText());
            sb.append("  `").append(columnName).append("` ").append(columnType).append(",\n");
            // Simplified PK detection
            if ("id".equalsIgnoreCase(columnName)) {
                pkColumns.add(columnName);
            }
        }
        
        if (!pkColumns.isEmpty()) {
            sb.append("  PRIMARY KEY (").append(pkColumns.stream().map(pk -> "`" + pk + "`").collect(Collectors.joining(", "))).append(")\n");
        } else {
            sb.setLength(sb.length() - 2); // Remove last comma
            sb.append("\n");
        }

        sb.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");
        LOG.debug("Generated CREATE TABLE statement from message schema: {}", sb.toString());
        return sb.toString();
    }
    
    private String generateCreateTableSqlFromSourceDb(String db, String table) throws SQLException {
        DatabaseMetaData metaData = sourceConnection.getMetaData();
        StringBuilder sb = new StringBuilder("CREATE TABLE `").append(this.targetDatabase).append("`.`").append(table).append("` (\n");

        // Get columns and types
        try (ResultSet columns = metaData.getColumns(db, null, table, null)) {
            while (columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                String typeName = columns.getString("TYPE_NAME");
                int columnSize = columns.getInt("COLUMN_SIZE");
                String isNullable = columns.getString("IS_NULLABLE");
                
                sb.append("  `").append(columnName).append("` ").append(typeName);
                if (typeName.equalsIgnoreCase("VARCHAR") || typeName.equalsIgnoreCase("CHAR")) {
                    sb.append("(").append(columnSize).append(")");
                }
                if ("NO".equalsIgnoreCase(isNullable)) {
                    sb.append(" NOT NULL");
                }
                sb.append(",\n");
            }
        }

        // Get primary keys
        List<String> pkColumns = getPrimaryKeys(db, table, null);
        if (!pkColumns.isEmpty()) {
            sb.append("  PRIMARY KEY (").append(pkColumns.stream().map(pk -> "`" + pk + "`").collect(Collectors.joining(", "))).append(")\n");
        } else {
            sb.setLength(sb.length() - 2); // Remove last comma
            sb.append("\n");
        }

        sb.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");
        LOG.debug("Generated CREATE TABLE statement from source DB metadata: {}", sb.toString());
        return sb.toString();
    }
    
    private String getMysqlType(String debeziumType) {
        // This mapping is simplified and may need to be expanded.
        switch (debeziumType) {
            case "int8": return "TINYINT";
            case "int16": return "SMALLINT";
            case "int32": return "INT";
            case "int64": return "BIGINT";
            case "float32": return "FLOAT";
            case "float64": return "DOUBLE";
            case "boolean": return "BOOLEAN";
            case "string": return "VARCHAR(255)";
            case "bytes": return "BLOB";
            default: return "TEXT"; // Fallback
        }
    }

    private void handleUpsert(String tableName, List<String> primaryKeys, JsonNode data) throws SQLException {
        if (data.isMissingNode() || !data.isObject()) return;

        List<String> columns = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        data.fields().forEachRemaining(field -> {
            columns.add(field.getKey());
            values.add(toObject(field.getValue()));
        });

        if (columns.isEmpty()) return;

        List<String> quotedColumns = columns.stream().map(c -> "`" + c + "`").collect(Collectors.toList());

        StringBuilder sql = new StringBuilder("INSERT INTO ").append(tableName)
                .append(" (").append(String.join(", ", quotedColumns)).append(") VALUES (")
                .append(columns.stream().map(c -> "?").collect(Collectors.joining(", ")))
                .append(") ON DUPLICATE KEY UPDATE ");

        String updateAssignments = columns.stream()
                .filter(col -> !primaryKeys.contains(col))
                .map(col -> "`" + col + "`" + " = VALUES(`" + col + "`)")
                .collect(Collectors.joining(", "));

        if (updateAssignments.isEmpty() && !columns.isEmpty()) {
            String firstCol = columns.get(0);
            updateAssignments = "`" + firstCol + "`" + " = VALUES(`" + firstCol + "`)";
        }

        sql.append(updateAssignments);

        LOG.debug("Executing UPSERT SQL: {}", sql);
        LOG.debug("Parameters for UPSERT: {}", values);

        // Get schema for type conversion
        String[] dbAndTable = tableName.replace("`", "").split("\\.");
        Map<String, Integer> schema = getTableSchema(dbAndTable[0], dbAndTable[1]);

        try (PreparedStatement ps = targetConnection.prepareStatement(sql.toString())) {
            for (int i = 0; i < values.size(); i++) {
                String columnName = columns.get(i);
                Object value = values.get(i);
                setParameter(ps, i + 1, value, columnName, schema);
            }
            ps.executeUpdate();
        }
    }

    private void handleDelete(String tableName, List<String> primaryKeys, JsonNode data) throws SQLException {
        if (data.isMissingNode() || primaryKeys.isEmpty()) {
            LOG.warn("Cannot process DELETE: primary key columns are missing or not found. PKs needed: {}, Data: {}", primaryKeys, data);
            return;
        }
        
        StringBuilder sql = new StringBuilder("DELETE FROM ").append(tableName).append(" WHERE ");
        List<Object> pkValues = new ArrayList<>();

        for (int i = 0; i < primaryKeys.size(); i++) {
            String pkColumn = primaryKeys.get(i);
            JsonNode pkValueNode = data.get(pkColumn);
            if (pkValueNode == null || pkValueNode.isNull()) {
                 LOG.error("Primary key column '{}' is null in DELETE event. Cannot proceed. Data: {}", pkColumn, data);
                 return;
            }
            sql.append("`").append(pkColumn).append("` = ?");
            if (i < primaryKeys.size() - 1) {
                sql.append(" AND ");
            }
            pkValues.add(toObject(pkValueNode));
        }

        LOG.debug("Executing DELETE SQL: {}", sql);
        LOG.debug("Parameters for DELETE: {}", pkValues);
        try (PreparedStatement ps = targetConnection.prepareStatement(sql.toString())) {
            for (int i = 0; i < pkValues.size(); i++) {
                ps.setObject(i + 1, pkValues.get(i));
            }
            ps.executeUpdate();
        }
    }
    
    private void setParameter(PreparedStatement ps, int index, Object value, String columnName, Map<String, Integer> schema) throws SQLException {
        int sqlType = schema.getOrDefault(columnName, Types.VARCHAR); // Default to VARCHAR if schema not found

        if (value instanceof String) {
            switch (sqlType) {
                case Types.BIGINT:
                case Types.INTEGER:
                case Types.SMALLINT:
                case Types.TINYINT:
                    try {
                        // Debezium may encode some numeric types as Base64 strings.
                        byte[] decodedBytes = Base64.getDecoder().decode((String) value);
                        BigInteger bigInt = new BigInteger(1, decodedBytes);
                        ps.setObject(index, bigInt);
                        LOG.debug("Decoded Base64 string '{}' to BigInteger '{}' for column '{}'", value, bigInt, columnName);
                        return;
                    } catch (IllegalArgumentException e) {
                        // Not a valid Base64 string, let the driver handle it (it will likely fail, which is correct)
                        LOG.debug("Value '{}' for column '{}' is not Base64; proceeding with standard setObject.", value, columnName);
                    }
                    break;
            }
        }
        
        ps.setObject(index, value);
    }

    private Map<String, Integer> getTableSchema(String database, String table) throws SQLException {
        String qualifiedTableName = database + "." + table;
        if (tableSchemaCache.containsKey(qualifiedTableName)) {
            return tableSchemaCache.get(qualifiedTableName);
        }

        Map<String, Integer> schema = new HashMap<>();
        DatabaseMetaData metaData = targetConnection.getMetaData();
        try (ResultSet rs = metaData.getColumns(database, null, table, null)) {
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                int dataType = rs.getInt("DATA_TYPE");
                schema.put(columnName, dataType);
            }
        }
        tableSchemaCache.put(qualifiedTableName, schema);
        LOG.info("Cached schema for table {}: {}", qualifiedTableName, schema);
        return schema;
    }

    private String extractDatabaseName(String jdbcUrl) {
        try {
            int lastSlash = jdbcUrl.lastIndexOf('/');
            int questionMark = jdbcUrl.indexOf('?');

            if (lastSlash != -1) {
                if (questionMark != -1 && questionMark > lastSlash) {
                    return jdbcUrl.substring(lastSlash + 1, questionMark);
                } else {
                    return jdbcUrl.substring(lastSlash + 1);
                }
            }
        } catch (Exception e) {
            LOG.warn("Could not parse database name from JDBC URL: {}. Operations may fail if schema is not specified.", jdbcUrl, e);
        }
        throw new IllegalArgumentException("Could not determine database name from JDBC URL. Please specify it in the URL (e.g., jdbc:mysql://host:port/database).");
    }

    private Object toObject(JsonNode jsonNode) {
        if (jsonNode == null || jsonNode.isNull()) return null;
        if (jsonNode.isBoolean()) return jsonNode.asBoolean();
        if (jsonNode.isDouble()) return jsonNode.asDouble();
        if (jsonNode.isInt()) return jsonNode.asInt();
        if (jsonNode.isLong()) return jsonNode.asLong();
        if (jsonNode.isTextual()) return jsonNode.asText();
        return jsonNode.toString();
    }
}