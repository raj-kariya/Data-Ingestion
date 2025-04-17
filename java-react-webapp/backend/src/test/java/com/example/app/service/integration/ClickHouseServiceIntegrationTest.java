package com.example.app.service;

import com.example.app.model.ColumnInfo;
import com.example.app.model.ConnectionConfig;
import com.example.app.model.TableInfo;
import com.example.app.util.TestHelper;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Integration tests for ClickHouseService.
 * 
 * Note: These tests require a running ClickHouse instance.
 * Add @Ignore annotation to skip these tests if ClickHouse is not available.
 */
public class ClickHouseServiceIntegrationTest {

    private ClickHouseService clickHouseService;
    private ConnectionConfig validConfig;
    
    // @Before
    // public void setUp() {
    //     clickHouseService = new ClickHouseService();
        
    //     // Set up a valid connection config
    //     // Replace with actual test server details
    //     validConfig = new ConnectionConfig();
    //     validConfig.setHost("localhost");  // Replace with your ClickHouse test server
    //     validConfig.setPort(8123);         // Default HTTP port
    //     validConfig.setDatabase("default"); // Default database
    //     validConfig.setUser("default");    // Default user
    //     // JWT Token might be required for your test environment
    //     // validConfig.setJwtToken("your-jwt-token");
    // }
    @Before
    public void setUp() {
        clickHouseService = new ClickHouseService();
        // Get connection config from test helper
        validConfig = TestHelper.getClickHouseTestConfig();

        // Create test tables before running tests
        TestHelper.setupTestTables(validConfig);
    }
    
    @Test
    public void testConnection_ValidCredentials_ReturnsTrue() {
        // This test requires a running ClickHouse instance
        assertTrue("Should connect successfully with valid credentials", 
                clickHouseService.testConnection(validConfig));
    }
    
    @Test
    public void testConnection_InvalidHost_ReturnsFalse() {
        ConnectionConfig invalidConfig = new ConnectionConfig();
        invalidConfig.setHost("nonexistent-host");
        invalidConfig.setPort(8123);
        invalidConfig.setDatabase("default");
        invalidConfig.setUser("default");
        
        assertFalse("Should fail to connect with invalid host", 
                clickHouseService.testConnection(invalidConfig));
    }
    
    @Test
    public void testGetTables_ValidConnection_ReturnsTables() throws SQLException {
        // This test requires a running ClickHouse instance with tables
        List<String> tables = clickHouseService.getTables(validConfig);
        
        assertNotNull("Tables list should not be null", tables);
        // If you have specific tables in your test database, you can assert they exist
        // assertTrue("Should contain test_table", tables.contains("test_table"));
    }
    
    @Test
    public void testGetTableSchema_ValidTable_ReturnsCorrectSchema() throws SQLException {
        // Assumes a table called "test_table" exists with specific columns
        // Modify table name as needed for your test environment
        String testTableName = "test_table"; 
        
        TableInfo tableInfo = clickHouseService.getTableSchema(validConfig, testTableName);
        
        assertNotNull("TableInfo should not be null", tableInfo);
        assertEquals("Table name should match", testTableName, tableInfo.getTableName());
        assertNotNull("Columns should not be null", tableInfo.getColumns());
        // If you know the expected columns, assert they exist
        // assertTrue("Should contain column 'id'", 
        //     tableInfo.getColumns().stream().anyMatch(c -> "id".equals(c.getName())));
    }
    
    @Test
    public void testQueryData_ValidQuery_ReturnsData() throws SQLException {
        // Assumes a table with data exists
        // Modify table name and column names as needed
        String testTableName = "test_table";
        List<String> columns = Arrays.asList("id", "name");
        
        List<List<Object>> data = clickHouseService.queryData(validConfig, testTableName, columns);
        
        assertNotNull("Data should not be null", data);
        // If test table is guaranteed to have data, assert it's not empty
        // assertFalse("Data should not be empty", data.isEmpty());
    }
    
    @Test
    public void testInsertData_ValidData_InsertsSuccessfully() throws SQLException {
        // Assumes a writable table exists
        // Modify table name, column names and test data as needed
        String testTableName = "test_insert_table";
        List<String> columns = Arrays.asList("id", "name");
        
        // Create test data
        List<List<Object>> testData = new ArrayList<>();
        List<Object> row1 = Arrays.asList(1, "Test Name 1");
        List<Object> row2 = Arrays.asList(2, "Test Name 2");
        testData.add(row1);
        testData.add(row2);
        
        int rowsInserted = clickHouseService.insertData(validConfig, testTableName, columns, testData);
        
        assertEquals("Should insert all rows", testData.size(), rowsInserted);
        
        // Optionally verify the data was inserted by querying it back
        List<List<Object>> queriedData = clickHouseService.queryData(validConfig, testTableName, columns);
        assertNotNull("Queried data should not be null", queriedData);
        // Note: This might not work if the table already has data or if ClickHouse doesn't
        // guarantee the order of returned rows. You might need a more sophisticated check.
    }
}