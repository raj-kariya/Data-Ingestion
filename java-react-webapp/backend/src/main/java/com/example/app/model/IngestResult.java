package com.example.app.model;

import java.util.UUID;

public class IngestResult {
    private boolean success;
    private int recordsProcessed;
    private String message;
    private long executionTimeMs;
    private String operationId;
    private int totalRecords;
    private int estimatedTotal;
    private double recordsPerSecond;
    private String status = "running"; // "running", "completed", "error"
    private long startTime;
    
    public IngestResult() {
        this.operationId = UUID.randomUUID().toString();
        this.startTime = System.currentTimeMillis();
    }
    
    public IngestResult(boolean success, int recordsProcessed, String message) {
        this.success = success;
        this.recordsProcessed = recordsProcessed;
        this.message = message;
        this.operationId = UUID.randomUUID().toString();
        this.startTime = System.currentTimeMillis();
    }
    
    public boolean isSuccess() {
        return success;
    }
    
    public void setSuccess(boolean success) {
        this.success = success;
    }
    
    public int getRecordsProcessed() {
        return recordsProcessed;
    }
    
    public void setRecordsProcessed(int recordsProcessed) {
        this.recordsProcessed = recordsProcessed;
    }
    
    public String getMessage() {
        return message;
    }
    
    public void setMessage(String message) {
        this.message = message;
    }
    
    public long getExecutionTimeMs() {
        return executionTimeMs;
    }
    
    public void setExecutionTimeMs(long executionTimeMs) {
        this.executionTimeMs = executionTimeMs;
    }

    public String getOperationId() {
        return operationId;
    }
    
    public void setOperationId(String operationId) {
        this.operationId = operationId;
    }
    
    public int getTotalRecords() {
        return totalRecords;
    }
    
    public void setTotalRecords(int totalRecords) {
        this.totalRecords = totalRecords;
    }
    
    public int getEstimatedTotal() {
        return estimatedTotal;
    }
    
    public void setEstimatedTotal(int estimatedTotal) {
        this.estimatedTotal = estimatedTotal;
    }
    
    public double getRecordsPerSecond() {
        long elapsedSecs = Math.max(1, (System.currentTimeMillis() - startTime) / 1000);
        return (double) recordsProcessed / elapsedSecs;
    }
    
    public void setRecordsPerSecond(double recordsPerSecond) {
        this.recordsPerSecond = recordsPerSecond;
    }
    
    public String getStatus() {
        return status;
    }
    
    public void setStatus(String status) {
        this.status = status;
    }
    
    public long getStartTime() {
        return startTime;
    }
    
    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }
}