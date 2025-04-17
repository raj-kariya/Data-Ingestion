package com.example.app.service;

import com.example.app.model.ConnectionConfig;
import com.example.app.model.IngestResult;
import java.util.concurrent.atomic.*;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;

public class IngestService {
    private final ClickHouseService clickHouseService;
    private final FlatFileService flatFileService;
    
    // Static map to store ongoing operations
    private static final Map<String, IngestResult> activeOperations = new ConcurrentHashMap<>();
    private static final long OPERATION_TIMEOUT = 3600000; // 1 hour in milliseconds
    
    public IngestService(ClickHouseService clickHouseService, FlatFileService flatFileService) {
        this.clickHouseService = clickHouseService;
        this.flatFileService = flatFileService;
    }
    
    /**
     * Periodically clean up completed operations that are older than the timeout
     */
    private void cleanupOldOperations() {
        long cutoffTime = System.currentTimeMillis() - OPERATION_TIMEOUT;
        activeOperations.entrySet().removeIf(entry -> {
            IngestResult result = entry.getValue();
            return ("completed".equals(result.getStatus()) || "error".equals(result.getStatus()))
                && (result.getStartTime() + result.getExecutionTimeMs() < cutoffTime);
        });
    }
    
    /**
     * Get the status of an operation by its ID
     */
    public IngestResult getOperationStatus(String operationId) {
        cleanupOldOperations();
        return activeOperations.get(operationId);
    }

    // Update an operation with new data
    public void updateOperation(IngestResult result) {
        activeOperations.put(result.getOperationId(), result);
    }
    
    /**
     * Update an operation's progress
     */
    private void updateProgress(IngestResult result, int incrementBy) {
        result.setRecordsProcessed(result.getRecordsProcessed() + incrementBy);
    }
    
    /**
     * Track a new operation
     */
    public void trackOperation(IngestResult result) {
        activeOperations.put(result.getOperationId(), result);
    }
    
    /**
     * Ingests data from ClickHouse to a Flat File
     */
    public IngestResult ingestFromClickHouseToFile(
            ConnectionConfig config, 
            String tableName,
            List<String> selectedColumns,
            String targetFilePath) {
        
        IngestResult result = new IngestResult();
        result.setStatus("running");
        trackOperation(result);
        long startTime = System.currentTimeMillis();
        
        try {
            // Try to get total row count for progress tracking
            try {
                int totalCount = clickHouseService.getTableRowCount(config, tableName);
                result.setTotalRecords(totalCount);
            } catch (Exception e) {
                // If count fails, we'll just proceed without total count
                System.out.println("Could not get row count: " + e.getMessage());
            }
            
            // Query data from ClickHouse
            List<List<Object>> data = clickHouseService.queryData(
                    config, tableName, selectedColumns);
            
            // Write to flat file (default delimiter is comma)
            int recordsWritten = flatFileService.writeToFile(
                    targetFilePath, selectedColumns, data, ",");
            
            result.setSuccess(true);
            result.setRecordsProcessed(recordsWritten);
            result.setMessage("Successfully exported " + recordsWritten + 
                    " records from ClickHouse to " + targetFilePath);
            result.setStatus("completed");
        } catch (Exception e) {
            result.setSuccess(false);
            result.setRecordsProcessed(0);
            result.setMessage("Failed to export data: " + e.getMessage());
            result.setStatus("error");
        }
        
        result.setExecutionTimeMs(System.currentTimeMillis() - startTime);
        return result;
    }
    
    /**
     * Ingests data from a Flat File to ClickHouse
     */
    public IngestResult ingestFromFileToClickHouse(
            String sourceFilePath,
            ConnectionConfig config,
            String tableName,
            List<String> selectedColumns) {
        
        IngestResult result = new IngestResult();
        result.setStatus("running");
        trackOperation(result);
        long startTime = System.currentTimeMillis();
        
        try {
            // Read data from flat file (default delimiter is comma)
            List<List<Object>> data = flatFileService.readFileData(
                    sourceFilePath, ",", selectedColumns);
            
            // Set total records for progress tracking
            result.setTotalRecords(data.size());
            
            // Insert into ClickHouse
            int recordsInserted = clickHouseService.insertData(
                    config, tableName, selectedColumns, data);
            
            result.setSuccess(true);
            result.setRecordsProcessed(recordsInserted);
            result.setMessage("Successfully imported " + recordsInserted + 
                    " records from " + sourceFilePath + " to ClickHouse");
            result.setStatus("completed");
        } catch (Exception e) {
            result.setSuccess(false);
            result.setRecordsProcessed(0);
            result.setMessage("Failed to import data: " + e.getMessage());
            result.setStatus("error");
        }
        
        result.setExecutionTimeMs(System.currentTimeMillis() - startTime);
        return result;
    }
    
    /**
     * Ingests data from a Flat File to ClickHouse using streaming for better progress tracking
     */
    public IngestResult streamFromFileToClickHouse(
            String sourceFilePath,
            ConnectionConfig config,
            String tableName,
            List<String> selectedColumns,
            String operationId) {
        
        // Get the existing operation to update
        final IngestResult result = activeOperations.get(operationId);
        if (result == null) {
            // If not found, create a new one (shouldn't happen)
            throw new IllegalStateException("Operation ID not found: " + operationId);
        }
        
        final long startTime = System.currentTimeMillis();
        
        try {
            // For better progress tracking, first count total lines
            try {
                int totalLines = flatFileService.countLines(sourceFilePath);
                // Subtract 1 for header row
                result.setTotalRecords(totalLines > 0 ? totalLines - 1 : 0);
                // Update the operation with total count
                updateOperation(result);
            } catch (Exception e) {
                // If we can't count, just continue
                System.err.println("Could not count lines in file: " + e.getMessage());
            }
            
            // Process data in batches for more frequent updates
            final int batchSize = 1000;
            final AtomicInteger recordsProcessed = new AtomicInteger(0);
            final long startTimeMs = System.currentTimeMillis();
            final ConnectionConfig configFinal = config;
            final String tableNameFinal = tableName;
            final List<String> selectedColumnsFinal = new ArrayList<>(selectedColumns);
            // This method should process file data in batches and call the consumer for each batch
            flatFileService.processFileDataInBatches(
                    sourceFilePath, ",", selectedColumnsFinal, batchSize, 
                    batch -> {
                        try {
                            // Insert batch into ClickHouse
                            int inserted = clickHouseService.insertData(
                                configFinal, tableNameFinal, selectedColumnsFinal, batch);
                            recordsProcessed.addAndGet(inserted);
                            
                            // Update progress
                            result.setRecordsProcessed(recordsProcessed.get());
                            result.setRecordsPerSecond(
                                    (double) recordsProcessed.get() / 
                                    Math.max(1, (System.currentTimeMillis() - startTimeMs) / 1000)
                            );
                            
                            // Update the operation
                            updateOperation(result);
                        } catch (Exception e) {
                            System.err.println("Failed to insert batch: " + e.getMessage());
                            throw new RuntimeException("Failed to insert batch: " + e.getMessage(), e);
                        }
                    });
            
            System.out.println("Processed " + recordsProcessed.get() + 
                    " records from " + sourceFilePath + " to ClickHouse");

            // Finalize the result
            result.setSuccess(true);
            result.setMessage("Successfully imported " + recordsProcessed.get() + 
                    " records from " + sourceFilePath + " to ClickHouse");
            result.setStatus("completed");
        } catch (Exception e) {
            e.printStackTrace();
                
            // Create a detailed error message
            String errorMessage = e.getMessage();
            Throwable cause = e.getCause();
            if (cause != null) {
                errorMessage += " (Cause: " + cause.getMessage() + ")";
            }
            
            // Important: Keep any progress we've made instead of resetting to 0
            // Don't reset recordsProcessed to 0 here
            
            result.setSuccess(false);
            result.setMessage("Failed to import data: " + errorMessage);
            result.setStatus("error");
            
            // Make sure to update the operation in the map
            updateOperation(result);
        }
        
        result.setExecutionTimeMs(System.currentTimeMillis() - startTime);
        return result;
    }
    
    // Similarly update the streamFromClickHouseToFile method
    public IngestResult streamFromClickHouseToFile(
            ConnectionConfig config, 
            String tableName,
            List<String> selectedColumns,
            String targetFilePath,
            String operationId) {
        
        // Get the existing operation to update
        IngestResult result = activeOperations.get(operationId);
        if (result == null) {
            // If not found, create a new one (shouldn't happen)
            result = new IngestResult();
            result.setOperationId(operationId);
        }
        
        long startTime = System.currentTimeMillis();
        
        try {
            // Try to get total row count
            try {
                int totalCount = clickHouseService.getTableRowCount(config, tableName);
                result.setTotalRecords(totalCount);
                // Update the operation with total count
                updateOperation(result);
            } catch (Exception e) {
                System.err.println("Could not get row count: " + e.getMessage());
            }
            
            // Stream data in batches with progress updates
            int batchSize = 10000;
            int offset = 0;
            int totalRecords = 0;
            List<List<Object>> allData = new ArrayList<>();
            
            // Process in batches for more frequent progress updates
            while (true) {
                StringBuilder queryBuilder = new StringBuilder("SELECT ");
                for (int i = 0; i < selectedColumns.size(); i++) {
                    if (i > 0) queryBuilder.append(", ");
                    queryBuilder.append("`").append(selectedColumns.get(i)).append("`");
                }
                queryBuilder.append(" FROM `").append(tableName).append("`")
                        .append(" LIMIT ").append(batchSize)
                        .append(" OFFSET ").append(offset);
                
                List<List<Object>> batch = clickHouseService.executeQuery(config, queryBuilder.toString());
                
                if (batch != null && !batch.isEmpty()) {
                    allData.addAll(batch);
                    totalRecords += batch.size();
                    
                    // Update progress
                    result.setRecordsProcessed(totalRecords);
                    result.setRecordsPerSecond(
                            (double) totalRecords / 
                            Math.max(1, (System.currentTimeMillis() - startTime) / 1000)
                    );
                    
                    // Update the operation
                    updateOperation(result);
                    
                    if (batch.size() < batchSize) {
                        // Reached the end
                        break;
                    }
                    
                    offset += batch.size();
                } else {
                    // No more data
                    break;
                }
            }
            
            // Write to file
            int recordsWritten = flatFileService.writeToFile(
                    targetFilePath, selectedColumns, allData, ",");
            
            // Finalize the result
            result.setSuccess(true);
            result.setRecordsProcessed(recordsWritten);
            result.setMessage("Successfully exported " + recordsWritten + 
                    " records from ClickHouse to " + targetFilePath);
            result.setStatus("completed");
        } catch (Exception e) {
            result.setSuccess(false);
            result.setRecordsProcessed(0);
            result.setMessage("Failed to export data: " + e.getMessage());
            result.setStatus("error");
        }
        
        result.setExecutionTimeMs(System.currentTimeMillis() - startTime);
        return result;
    }
}