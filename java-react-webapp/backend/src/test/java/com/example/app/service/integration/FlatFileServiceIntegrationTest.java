package com.example.app.service.integration;

import com.example.app.model.ColumnInfo;
import com.example.app.model.TableInfo;
import com.example.app.service.FlatFileService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class FlatFileServiceIntegrationTest {

    private FlatFileService flatFileService;
    private String testFilePath;
    private String outputFilePath;
    
    @Before
    public void setUp() throws IOException {
        flatFileService = new FlatFileService();
        
        // Create a temporary test CSV file
        testFilePath = createTestCsvFile();
        outputFilePath = System.getProperty("java.io.tmpdir") + File.separator + "output_test.csv";
    }
    
    @After
    public void tearDown() {
        // Clean up test files
        deleteFileIfExists(testFilePath);
        deleteFileIfExists(outputFilePath);
    }
    
    @Test
    public void testReadFileSchema_ValidFile_ReturnsCorrectSchema() throws IOException {
        TableInfo tableInfo = flatFileService.readFileSchema(testFilePath, ",");
        
        assertNotNull("TableInfo should not be null", tableInfo);
        assertEquals("Table name should be test_data", "test_data", tableInfo.getTableName());
        
        List<ColumnInfo> columns = tableInfo.getColumns();
        assertNotNull("Columns should not be null", columns);
        assertEquals("Should have 3 columns", 3, columns.size());
        
        // Check column names
        assertEquals("First column should be 'id'", "id", columns.get(0).getName());
        assertEquals("Second column should be 'name'", "name", columns.get(1).getName());
        assertEquals("Third column should be 'value'", "value", columns.get(2).getName());
        
        // Check inferred types
        assertEquals("id should be Int32", "Int32", columns.get(0).getType());
        assertEquals("name should be String", "String", columns.get(1).getType());
        assertEquals("value should be Float64", "Float64", columns.get(2).getType());
    }
    
    @Test
    public void testReadFileData_ValidFile_ReturnsCorrectData() throws IOException {
        List<String> selectedColumns = Arrays.asList("id", "name", "value");
        List<List<Object>> data = flatFileService.readFileData(testFilePath, ",", selectedColumns);
        
        assertNotNull("Data should not be null", data);
        assertEquals("Should have 3 rows", 3, data.size());
        
        // Check first row
        List<Object> row1 = data.get(0);
        assertEquals("First row should have 3 columns", 3, row1.size());
        assertEquals("First column of first row should be '1'", "1", row1.get(0));
        assertEquals("Second column of first row should be 'John'", "John", row1.get(1));
        assertEquals("Third column of first row should be '10.5'", "10.5", row1.get(2));
    }
    
    @Test
    public void testReadFileData_SelectedColumns_ReturnsOnlySelectedColumns() throws IOException {
        List<String> selectedColumns = Arrays.asList("id", "value");
        List<List<Object>> data = flatFileService.readFileData(testFilePath, ",", selectedColumns);
        
        assertNotNull("Data should not be null", data);
        assertEquals("Should have 3 rows", 3, data.size());
        
        // Check first row
        List<Object> row1 = data.get(0);
        assertEquals("First row should have 2 columns", 2, row1.size());
        assertEquals("First column of first row should be '1'", "1", row1.get(0));
        assertEquals("Second column of first row should be '10.5'", "10.5", row1.get(1));
    }
    
    @Test
    public void testWriteToFile_ValidData_WritesSuccessfully() throws IOException {
        List<String> headers = Arrays.asList("id", "name", "score");
        List<List<Object>> data = Arrays.asList(
            Arrays.asList(1, "Alice", 95.5),
            Arrays.asList(2, "Bob", 87.0),
            Arrays.asList(3, "Charlie", 92.3)
        );
        
        int rowsWritten = flatFileService.writeToFile(outputFilePath, headers, data, ",");
        
        assertEquals("Should write 3 rows", 3, rowsWritten);
        
        // Verify file exists
        File outputFile = new File(outputFilePath);
        assertTrue("Output file should exist", outputFile.exists());
        
        // Read the file back and verify content
        List<String> lines = Files.readAllLines(Paths.get(outputFilePath));
        assertEquals("Should have 4 lines (1 header + 3 data)", 4, lines.size());
        assertEquals("Header should match", "id,name,score", lines.get(0));
        assertEquals("First data row should match", "1,Alice,95.5", lines.get(1));
    }
    
    @Test
    public void testRoundTrip_ReadWriteRead_DataIntegrity() throws IOException {
        // First read data from test file
        List<String> columns = Arrays.asList("id", "name", "value");
        List<List<Object>> originalData = flatFileService.readFileData(testFilePath, ",", columns);
        
        // Write it to output file
        flatFileService.writeToFile(outputFilePath, columns, originalData, ",");
        
        // Read it back
        List<List<Object>> readBackData = flatFileService.readFileData(outputFilePath, ",", columns);
        
        // Verify data integrity
        assertEquals("Number of rows should be preserved", originalData.size(), readBackData.size());
        
        for (int i = 0; i < originalData.size(); i++) {
            List<Object> originalRow = originalData.get(i);
            List<Object> readBackRow = readBackData.get(i);
            
            assertEquals("Row " + i + " should have same number of columns", 
                    originalRow.size(), readBackRow.size());
            
            for (int j = 0; j < originalRow.size(); j++) {
                assertEquals("Value at row " + i + ", column " + j + " should be preserved",
                        originalRow.get(j), readBackRow.get(j));
            }
        }
    }
    
    // Helper method to create a test CSV file
    private String createTestCsvFile() throws IOException {
        // Create a file in the temp directory with a fixed name
        String tempDir = System.getProperty("java.io.tmpdir");
        File testFile = new File(tempDir, "test_data.csv");
        
        // Make sure to delete any existing file first
        if (testFile.exists()) {
            testFile.delete();
        }
        
        try (FileWriter writer = new FileWriter(testFile)) {
            // Write header
            writer.write("id,name,value\n");
            // Write data rows
            writer.write("1,John,10.5\n");
            writer.write("2,Jane,20.7\n");
            writer.write("3,Bob,15.2\n");
        }
        
        return testFile.getAbsolutePath();
    }
    
    // Helper method to delete a file if it exists
    private void deleteFileIfExists(String filePath) {
        if (filePath != null) {
            try {
                Files.deleteIfExists(Paths.get(filePath));
            } catch (IOException e) {
                // Ignore errors on cleanup
            }
        }
    }
}