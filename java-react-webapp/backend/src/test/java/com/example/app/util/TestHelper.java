package com.example.app.util;

import com.clickhouse.jdbc.ClickHouseDataSource;
import com.example.app.model.ConnectionConfig;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class TestHelper {
    
    /**
     * Loads ClickHouse connection config from test properties file
     */
    public static ConnectionConfig getClickHouseTestConfig() {
        Properties props = new Properties();
        
        try (InputStream input = TestHelper.class.getClassLoader()
                .getResourceAsStream("clickhouse-test.properties")) {
            
            if (input == null) {
                System.out.println("Sorry, unable to find clickhouse-test.properties");
                // Return default config
                return getDefaultClickHouseConfig();
            }
            
            props.load(input);
            
            ConnectionConfig config = new ConnectionConfig();
            config.setHost(props.getProperty("clickhouse.host", "localhost"));
            config.setPort(Integer.parseInt(props.getProperty("clickhouse.port", "8123")));
            config.setDatabase(props.getProperty("clickhouse.database", "default"));
            config.setUser(props.getProperty("clickhouse.user", "default"));
            // JWT token would be set here if using JWT auth
            
            return config;
            
        } catch (IOException ex) {
            System.out.println("Error loading clickhouse-test.properties: " + ex.getMessage());
            return getDefaultClickHouseConfig();
        }
    }
    
    /**
     * Provides default connection config for local testing
     */
    private static ConnectionConfig getDefaultClickHouseConfig() {
        ConnectionConfig config = new ConnectionConfig();
        config.setHost("localhost");
        config.setPort(8123);  // Default HTTP port for ClickHouse
        config.setDatabase("default");
        config.setUser("default");
        return config;
    }

    /**
     * Creates test tables in ClickHouse for testing
     */
    public static void setupTestTables(ConnectionConfig config) {
        try (Connection connection = getConnection(config)) {
            // Create test_table if it doesn't exist
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("CREATE TABLE IF NOT EXISTS test_table (" +
                    "id Int32, name String, value Float64) ENGINE = Memory");
            }
                
            // Insert sample data if table is empty
            List<List<Object>> data = queryData(connection, "test_table", Arrays.asList("id"));
            if (data.isEmpty()) {
                List<String> columns = Arrays.asList("id", "name", "value");
                List<List<Object>> testData = Arrays.asList(
                    Arrays.asList(1, "John", 10.5),
                    Arrays.asList(2, "Jane", 20.7),
                    Arrays.asList(3, "Bob", 15.2)
                );
                insertData(connection, config.getDatabase(), "test_table", columns, testData);
            }
            
            // Create test_insert_table for insert tests
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("CREATE TABLE IF NOT EXISTS test_insert_table (" +
                    "id Int32, name String) ENGINE = Memory");
            }
                
        } catch (SQLException e) {
            System.err.println("Failed to set up test tables: " + e.getMessage());
        }
    }
    
    /**
     * Gets a connection to ClickHouse
     */
    public static Connection getConnection(ConnectionConfig config) throws SQLException {
        String url = String.format("jdbc:clickhouse://%s:%d/%s", 
                config.getHost(), config.getPort(), config.getDatabase());
        
        Properties properties = new Properties();
        properties.setProperty("user", config.getUser());
        
        // Disable compression to avoid LZ4 dependency issues
        properties.setProperty("compress", "0");
        
        // Using JWT token if provided
        if (config.getJwtToken() != null && !config.getJwtToken().isEmpty()) {
            properties.setProperty("password", config.getJwtToken());
            properties.setProperty("ssl", "true");
        }
        
        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);
        return dataSource.getConnection();
    }
    
    /**
     * Executes a query on ClickHouse directly from TestHelper
     */
    public static void executeQuery(ConnectionConfig config, String query) throws SQLException {
        try (Connection connection = getConnection(config);
             Statement stmt = connection.createStatement()) {
            stmt.execute(query);
        }
    }
    
    /**
     * Queries data from ClickHouse directly from TestHelper
     */
    private static List<List<Object>> queryData(Connection connection, String tableName, 
                                           List<String> columns) throws SQLException {
        List<List<Object>> data = new ArrayList<>();
        
        String columnList = String.join(", ", columns);
        String query = "SELECT " + columnList + " FROM " + tableName;
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            
            while (rs.next()) {
                List<Object> row = new ArrayList<>();
                for (int i = 1; i <= columns.size(); i++) {
                    row.add(rs.getObject(i));
                }
                data.add(row);
            }
        }
        
        return data;
    }
    
    /**
     * Inserts data into ClickHouse directly from TestHelper
     */
    private static int insertData(Connection connection, String database, String tableName, 
                             List<String> columns, List<List<Object>> data) throws SQLException {
        String columnList = String.join(", ", columns);
        StringBuilder placeholders = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) placeholders.append(", ");
            placeholders.append("?");
        }
        
        String sql = "INSERT INTO " + database + "." + tableName + 
                     " (" + columnList + ") VALUES (" + placeholders + ")";
        
        int rowsInserted = 0;
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            for (List<Object> row : data) {
                for (int i = 0; i < row.size(); i++) {
                    pstmt.setObject(i + 1, row.get(i));
                }
                
                rowsInserted += pstmt.executeUpdate();
            }
        }
        
        return rowsInserted;
    }
}