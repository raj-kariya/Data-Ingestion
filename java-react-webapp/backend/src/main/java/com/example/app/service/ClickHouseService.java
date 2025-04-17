package com.example.app.service;

import com.clickhouse.jdbc.ClickHouseDataSource;
import com.example.app.model.ColumnInfo;
import com.example.app.model.ConnectionConfig;
import com.example.app.model.TableInfo;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ClickHouseService {
    
    public boolean testConnection(ConnectionConfig config) {
        try (Connection connection = getConnection(config)) {
            return connection.isValid(5);
        } catch (SQLException e) {
            System.err.println("Connection test failed: " + e.getMessage());
            return false;
        }
    }
    
    public List<String> getTables(ConnectionConfig config) throws SQLException {
        List<String> tables = new ArrayList<>();
        
        try (Connection connection = getConnection(config);
             Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW TABLES FROM " + config.getDatabase())) {
            
            while (rs.next()) {
                tables.add(rs.getString(1));
            }
        }
        
        return tables;
    }
    
    // Also update the getTableSchema method for consistency:
    public TableInfo getTableSchema(ConnectionConfig config, String tableName) throws SQLException {
        TableInfo tableInfo = new TableInfo();
        tableInfo.setTableName(tableName);
        List<ColumnInfo> columns = new ArrayList<>();
        
        // Check if tableName already has database prefix
        String fullTableName;
        if (tableName.contains(".")) {
            // Table name already has database prefix
            fullTableName = tableName;
        } else {
            // Add database prefix
            fullTableName = config.getDatabase() + "." + tableName;
        }
        
        try (Connection connection = getConnection(config);
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("DESCRIBE " + fullTableName)) {
            
            while (rs.next()) {
                ColumnInfo column = new ColumnInfo();
                column.setName(rs.getString("name"));
                column.setType(rs.getString("type"));
                column.setSelected(true); // Default to selected
                columns.add(column);
            }
        }
        
        tableInfo.setColumns(columns);
        return tableInfo;
    }
    
    public List<List<Object>> queryData(ConnectionConfig config, String tableName, List<String> columns) throws SQLException {
        List<List<Object>> data = new ArrayList<>();
        
        String columnList = String.join(", ", columns);
        
        // Check if tableName already has database prefix
        String fullTableName;
        if (tableName.contains(".")) {
            // Table name already has database prefix
            fullTableName = tableName;
        } else {
            // Add database prefix
            fullTableName = config.getDatabase() + "." + tableName;
        }
        
        String query = "SELECT " + columnList + " FROM " + fullTableName;
        
        try (Connection connection = getConnection(config);
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(query)) {
            
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            
            while (rs.next()) {
                List<Object> row = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.add(rs.getObject(i));
                }
                data.add(row);
            }
        }
        
        return data;
    }
    
    public int insertData(ConnectionConfig config, String tableName, 
                        List<String> columns, List<List<Object>> data) throws SQLException {
        String columnList = String.join(", ", columns);
        StringBuilder placeholders = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) placeholders.append(", ");
            placeholders.append("?");
        }
        
        // Check if tableName already has database prefix
        String fullTableName;
        if (tableName.contains(".")) {
            // Table name already has database prefix
            fullTableName = tableName;
        } else {
            // Add database prefix
            fullTableName = config.getDatabase() + "." + tableName;
        }
        
        String sql = "INSERT INTO " + fullTableName + 
                    " (" + columnList + ") VALUES (" + placeholders + ")";
        
        int rowsInserted = 0;
        
        try (Connection connection = getConnection(config);
            PreparedStatement pstmt = connection.prepareStatement(sql)) {
            
            for (List<Object> row : data) {
                for (int i = 0; i < row.size(); i++) {
                    pstmt.setObject(i + 1, row.get(i));
                }
                
                rowsInserted += pstmt.executeUpdate();
            }
        }
        
        return rowsInserted;
    }
    private Connection getConnection(ConnectionConfig config) throws SQLException {
        String url = String.format("jdbc:clickhouse://%s:%d/%s", 
                config.getHost(), config.getPort(), config.getDatabase());
        
        Properties properties = new Properties();
        properties.setProperty("user", config.getUser());

        // // Disable compression to avoid LZ4 dependency issues
        // properties.setProperty("compress", "0");
        
        // Using JWT token if provided
        if (config.getJwtToken() != null && !config.getJwtToken().isEmpty()) {
            properties.setProperty("password", config.getJwtToken());
            properties.setProperty("ssl", "true");
            properties.setProperty("custom_http_params", "accept_encoding=gzip");
        }
        
        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);
        return dataSource.getConnection();
    }

    // Add this method to ClickHouseService.java
    public boolean createTable(ConnectionConfig config, String tableName, 
                            List<String> columns, String sourceFilePath) {
        try (Connection connection = getConnection(config)) {
            // Determine column types from the source file or use String as default
            StringBuilder createTableSQL = new StringBuilder();
            createTableSQL.append("CREATE TABLE IF NOT EXISTS ");
            
            // Check if tableName already has database prefix
            String fullTableName;
            if (tableName.contains(".")) {
                // Table name already has database prefix
                fullTableName = tableName;
            } else {
                // Add database prefix
                fullTableName = config.getDatabase() + "." + tableName;
            }
            
            createTableSQL.append(fullTableName);
            createTableSQL.append(" (");
            
            for (int i = 0; i < columns.size(); i++) {
                if (i > 0) {
                    createTableSQL.append(", ");
                }
                createTableSQL.append(columns.get(i)).append(" String");
            }
            
            createTableSQL.append(") ENGINE = MergeTree() ORDER BY tuple()");
            
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(createTableSQL.toString());
                return true;
            }
        } catch (SQLException e) {
            System.err.println("Failed to create table: " + e.getMessage());
            return false;
        }
    }

    public List<List<Object>> executeQuery(ConnectionConfig config, String query) throws SQLException {
        List<List<Object>> data = new ArrayList<>();
        
        try (Connection connection = getConnection(config);
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(query)) {
            
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            
            while (rs.next()) {
                List<Object> row = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.add(rs.getObject(i));
                }
                data.add(row);
            }
        }
        
        return data;
    }

    public int getTableRowCount(ConnectionConfig config, String tableName) throws SQLException {
        String query = "SELECT count(*) FROM `" + tableName + "`";
        List<List<Object>> result = executeQuery(config, query);
        if (result != null && !result.isEmpty() && !result.get(0).isEmpty()) {
            return ((Number)result.get(0).get(0)).intValue();
        }
        return 0;
    }
}