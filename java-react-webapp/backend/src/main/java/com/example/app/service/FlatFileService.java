package com.example.app.service;

import com.example.app.model.ColumnInfo;
import com.example.app.model.TableInfo;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class FlatFileService {

    private static final int DEFAULT_BATCH_SIZE = 1000;

    /**
     * Reads a CSV file and returns its schema
     */
    public TableInfo readFileSchema(String filePath, String delimiter) throws IOException {
        File file = new File(filePath);
        String fileName = FilenameUtils.getBaseName(file.getName());
        
        TableInfo tableInfo = new TableInfo();
        tableInfo.setTableName(fileName);
        
        List<ColumnInfo> columns = new ArrayList<>();
        
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(filePath), StandardCharsets.UTF_8)) {
            CSVFormat format = CSVFormat.DEFAULT.builder()
                .setDelimiter(delimiter.charAt(0))
                .setHeader()
                .setSkipHeaderRecord(true)
                .build();
            
            try (CSVParser csvParser = new CSVParser(reader, format)) {
                // Get header names
                Map<String, Integer> headerMap = csvParser.getHeaderMap();
                
                for (String header : headerMap.keySet()) {
                    ColumnInfo column = new ColumnInfo();
                    column.setName(header);
                    column.setType("String"); // Default to String without deeper inspection
                    column.setSelected(true);
                    columns.add(column);
                }
                
                // Try to infer data types from first record
                if (csvParser.iterator().hasNext()) {
                    CSVRecord record = csvParser.iterator().next();
                    
                    for (int i = 0; i < columns.size(); i++) {
                        ColumnInfo column = columns.get(i);
                        String value = record.get(i);
                        
                        // Simple type inference
                        if (isInteger(value)) {
                            column.setType("Int32");
                        } else if (isDouble(value)) {
                            column.setType("Float64");
                        } else if (isBoolean(value)) {
                            column.setType("Boolean");
                        } else {
                            column.setType("String");
                        }
                    }
                }
            }
        }
        
        tableInfo.setColumns(columns);
        return tableInfo;
    }
    
    /**
     * Reads data from a CSV file with selected columns
     */
    public List<List<Object>> readFileData(String filePath, String delimiter, List<String> selectedColumns) 
            throws IOException {
        List<List<Object>> data = new ArrayList<>();
        
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(filePath), StandardCharsets.UTF_8)) {
            CSVFormat format = CSVFormat.DEFAULT.builder()
                .setDelimiter(delimiter.charAt(0))
                .setHeader()
                .setSkipHeaderRecord(true)
                .build();
            
            try (CSVParser csvParser = new CSVParser(reader, format)) {
                Map<String, Integer> headerMap = csvParser.getHeaderMap();
                
                // Map selected columns to their indices
                Map<Integer, Integer> columnIndexMap = new HashMap<>();
                for (int i = 0; i < selectedColumns.size(); i++) {
                    String columnName = selectedColumns.get(i);
                    Integer sourceIndex = headerMap.get(columnName);
                    if (sourceIndex != null) {
                        columnIndexMap.put(i, sourceIndex);
                    }
                }
                
                // Read data
                for (CSVRecord record : csvParser) {
                    List<Object> row = new ArrayList<>();
                    
                    for (int i = 0; i < selectedColumns.size(); i++) {
                        Integer sourceIndex = columnIndexMap.get(i);
                        if (sourceIndex != null) {
                            row.add(record.get(sourceIndex));
                        } else {
                            row.add(null);
                        }
                    }
                    
                    data.add(row);
                }
            }
        }
        
        return data;
    }
    
    /**
     * Writes data to a CSV file
     */
    public int writeToFile(String filePath, List<String> headers, List<List<Object>> data, String delimiter) 
            throws IOException {
        File file = new File(filePath);
        
        // Create parent directory if it doesn't exist
        File parentDir = file.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            parentDir.mkdirs();
        }
        
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(filePath), StandardCharsets.UTF_8)) {
            CSVFormat format = CSVFormat.DEFAULT.builder()
                .setDelimiter(delimiter.charAt(0))
                .setHeader(headers.toArray(new String[0]))
                .build();
            
            try (CSVPrinter csvPrinter = new CSVPrinter(writer, format)) {
                // Write records
                for (List<Object> row : data) {
                    csvPrinter.printRecord(row);
                }
            }
        }
        
        return data.size();
    }
    
    private boolean isInteger(String s) {
        try {
            Integer.parseInt(s);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
    
    private boolean isDouble(String s) {
        try {
            Double.parseDouble(s);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
    
    private boolean isBoolean(String s) {
        return "true".equalsIgnoreCase(s) || "false".equalsIgnoreCase(s);
    }

    /**
     * Process file data in batches with a callback for each batch
     * Uses proper CSV parsing to handle quoted fields and other CSV complexities
     */
    public int processFileDataInBatches(
            String filePath, 
            String delimiter, 
            List<String> columns, 
            int batchSize,
            Consumer<List<List<Object>>> batchProcessor) throws IOException {
        
        int totalRecords = 0;
        
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(filePath), StandardCharsets.UTF_8)) {
            CSVFormat format = CSVFormat.DEFAULT.builder()
                .setDelimiter(delimiter.charAt(0))
                .setHeader()
                .setSkipHeaderRecord(true)
                .build();
            
            try (CSVParser csvParser = new CSVParser(reader, format)) {
                // Get header map
                Map<String, Integer> headerMap = csvParser.getHeaderMap();
                
                // Debug headers if needed
                System.out.println("Available columns in file: " + String.join(", ", headerMap.keySet()));
                System.out.println("Requested columns: " + String.join(", ", columns));
                
                // Validate that all requested columns exist
                List<String> missingColumns = new ArrayList<>();
                for (String column : columns) {
                    if (!headerMap.containsKey(column)) {
                        missingColumns.add(column);
                    }
                }
                
                if (!missingColumns.isEmpty()) {
                    System.out.println("Missing columns: " + String.join(", ", missingColumns));
                    throw new IOException("Column(s) not found in file: " + 
                            String.join(", ", missingColumns) + 
                            ". Available columns are: " + String.join(", ", headerMap.keySet()));
                }
                
                System.out.println("All requested columns are present in the file.");

                // Map column indices for more efficient lookup
                int[] columnIndices = new int[columns.size()];
                for (int i = 0; i < columns.size(); i++) {
                    columnIndices[i] = headerMap.get(columns.get(i));
                }
                
                // Process records in batches
                List<List<Object>> batch = new ArrayList<>(batchSize);
                
                System.out.println("Processing file in batches of " + batchSize + " records.");
                for (CSVRecord record : csvParser) {
                    List<Object> row = new ArrayList<>(columns.size());
                    
                    // Add each requested column in order
                    for (int i = 0; i < columns.size(); i++) {
                        int columnIndex = columnIndices[i];
                        String value = record.get(columnIndex);
                        row.add(value.isEmpty() ? null : value);
                    }
                    
                    batch.add(row);
                    totalRecords++;
                    
                    // Process batch when it reaches the batch size
                    if (batch.size() >= batchSize) {
                        batchProcessor.accept(new ArrayList<>(batch)); // Pass a copy to be safe
                        batch.clear();
                    }
                }
                
                // Process final batch if any
                if (!batch.isEmpty()) {
                    batchProcessor.accept(new ArrayList<>(batch));
                }
            }
        }
        
        System.out.println("Total records processed: " + totalRecords);
        return totalRecords;
    }

    public List<List<Object>> previewFileData(String filePath, String delimiter, List<String> selectedColumns, int maxRows) 
        throws IOException {
        List<List<Object>> data = new ArrayList<>();
        
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(filePath), StandardCharsets.UTF_8)) {
            CSVFormat format = CSVFormat.DEFAULT.builder()
                .setDelimiter(delimiter.charAt(0))
                .setHeader()
                .setSkipHeaderRecord(true)
                .build();
            
            try (CSVParser csvParser = new CSVParser(reader, format)) {
                Map<String, Integer> headerMap = csvParser.getHeaderMap();
                
                // Map selected columns to their indices
                Map<Integer, Integer> columnIndexMap = new HashMap<>();
                for (int i = 0; i < selectedColumns.size(); i++) {
                    String columnName = selectedColumns.get(i);
                    Integer sourceIndex = headerMap.get(columnName);
                    if (sourceIndex != null) {
                        columnIndexMap.put(i, sourceIndex);
                    }
                }
                
                // Read limited data
                int rowCount = 0;
                for (CSVRecord record : csvParser) {
                    if (rowCount >= maxRows) {
                        break;
                    }
                    
                    List<Object> row = new ArrayList<>();
                    
                    for (int i = 0; i < selectedColumns.size(); i++) {
                        Integer sourceIndex = columnIndexMap.get(i);
                        if (sourceIndex != null) {
                            row.add(record.get(sourceIndex));
                        } else {
                            row.add(null);
                        }
                    }
                    
                    data.add(row);
                    rowCount++;
                }
            }
        }
        
        return data;
    }

    public int countLines(String filePath) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            int count = 0;
            while (reader.readLine() != null) {
                count++;
            }
            return count;
        }
    }
}