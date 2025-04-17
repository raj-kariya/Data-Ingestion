package com.example.app.service.integration;

import com.example.app.model.ConnectionConfig;
import com.example.app.model.IngestResult;
import com.example.app.service.ClickHouseService;
import com.example.app.service.FlatFileService;
import com.example.app.service.IngestService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Integration tests for IngestService.
 * 
 * Note: These tests require a running ClickHouse instance.
 * Add @Ignore annotation to skip these tests if ClickHouse is not available.
 */
public class IngestServiceIntegrationTest {

    private IngestService ingestService;
    private ClickHouseService clickHouseService;
    private FlatFileService flatFileService;
    private ConnectionConfig testConfig;
    private String testFilePath;
    private String outputFilePath;
    private String testTableName = "test_ingest_table";
    
    @Before
    public void setUp() throws IOException, SQLException {
        clickHouseService = new ClickHouseService();
        flatFileService = new FlatFileService();
        ingestService = new IngestService(clickHouseService, flatFileService);
        
        // Set up test connection config
        testConfig = new ConnectionConfig();
        testConfig.setHost("localhost");  // Replace with your test server
        testConfig.setPort(8123);
        testConfig.setDatabase("default");
        testConfig.setUser("default");
        
        // Create test CSV file
        testFilePath = createTestCsvFile();
        outputFilePath = System.getProperty("java.io.tmpdir") + File.separator + "ingest_output.csv";
        
        // Ensure test table exists
        // Note: You need to create this table in your test ClickHouse instance
        // or modify this test to create it dynamically
    }
    
    @After
    public void tearDown() {
        // Clean up test files
        deleteFileIfExists(testFilePath);
        deleteFileIfExists(outputFilePath);
    }
    
    @Test
    @Ignore("Requires a running ClickHouse instance with test table")
    public void testIngestFromFileToClickHouse_ValidData_InsertsSuccessfully() {
        List<String> columns = Arrays.asList("id", "name", "value");
        
        IngestResult result = ingestService.ingestFromFileToClickHouse(
                testFilePath, testConfig, testTableName, columns);
        
        assertTrue("Ingestion should be successful", result.isSuccess());
        assertEquals("Should ingest 3 records", 3, result.getRecordsProcessed());
        assertNotNull("Should have a message", result.getMessage());
        assertTrue("Execution time should be tracked", result.getExecutionTimeMs() > 0);
    }
    
    @Test
    @Ignore("Requires a running ClickHouse instance with test table")
    public void testIngestFromClickHouseToFile_ValidQuery_ExportsSuccessfully() {
        // First ensure data exists in the test table
        // This could be from a previous test or pre-inserted test data
        
        List<String> columns = Arrays.asList("id", "name", "value");
        
        IngestResult result = ingestService.ingestFromClickHouseToFile(
                testConfig, testTableName, columns, outputFilePath);
        
        assertTrue("Ingestion should be successful", result.isSuccess());
        assertTrue("Should export at least one record", result.getRecordsProcessed() > 0);
        assertNotNull("Should have a message", result.getMessage());
        assertTrue("Execution time should be tracked", result.getExecutionTimeMs() > 0);
        
        // Verify file was created
        File outputFile = new File(outputFilePath);
        assertTrue("Output file should exist", outputFile.exists());
    }
    
    @Test
    public void testIngestFromFileToClickHouse_InvalidConnection_ReturnsErrorResult() {
        List<String> columns = Arrays.asList("id", "name", "value");
        
        ConnectionConfig invalidConfig = new ConnectionConfig();
        invalidConfig.setHost("nonexistent-host");
        invalidConfig.setPort(8123);
        invalidConfig.setDatabase("default");
        invalidConfig.setUser("default");
        
        IngestResult result = ingestService.ingestFromFileToClickHouse(
                testFilePath, invalidConfig, testTableName, columns);
        
        assertFalse("Ingestion should fail", result.isSuccess());
        assertEquals("Should process 0 records", 0, result.getRecordsProcessed());
        assertNotNull("Should have an error message", result.getMessage());
        assertTrue("Error message should indicate failure", 
                result.getMessage().contains("Failed to import data"));
    }
    
    @Test
    public void testIngestFromClickHouseToFile_InvalidFilePath_ReturnsErrorResult() {
        List<String> columns = Arrays.asList("id", "name", "value");
        
        String invalidPath = "/invalid/directory/that/does/not/exist/file.csv";
        
        IngestResult result = ingestService.ingestFromClickHouseToFile(
                testConfig, testTableName, columns, invalidPath);
        
        assertFalse("Ingestion should fail", result.isSuccess());
        assertEquals("Should process 0 records", 0, result.getRecordsProcessed());
        assertNotNull("Should have an error message", result.getMessage());
        assertTrue("Error message should indicate failure", 
                result.getMessage().contains("Failed to export data"));
    }
    
    // Helper method to create a test CSV file
    private String createTestCsvFile() throws IOException {
        Path tempFile = Files.createTempFile("ingest_test", ".csv");
        
        try (FileWriter writer = new FileWriter(tempFile.toFile())) {
            // Write header
            writer.write("id,name,value\n");
            // Write data rows
            writer.write("1,John,10.5\n");
            writer.write("2,Jane,20.7\n");
            writer.write("3,Bob,15.2\n");
        }
        
        return tempFile.toString();
    }
    
    // Helper method to delete a file if it exists
    private void deleteFileIfExists(String filePath) {
        if (filePath != null) {
            try {
                Files.deleteIfExists(Path.of(filePath));
            } catch (IOException e) {
                // Ignore errors on cleanup
            }
        }
    }
}