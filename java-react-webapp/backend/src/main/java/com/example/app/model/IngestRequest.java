package com.example.app.model;

import java.util.List;

public class IngestRequest {
    private String sourceType; // "ClickHouse" or "FlatFile"
    private ConnectionConfig connectionConfig;
    private String tableName;
    private List<String> selectedColumns;
    
    // For File to ClickHouse
    private String sourceFilePath;
    
    // For ClickHouse to File
    private String targetFilePath;
    private String delimiter = ",";  // Default delimiter
    
    public String getSourceType() {
        return sourceType;
    }
    
    public void setSourceType(String sourceType) {
        this.sourceType = sourceType;
    }
    
    public ConnectionConfig getConnectionConfig() {
        return connectionConfig;
    }
    
    public void setConnectionConfig(ConnectionConfig connectionConfig) {
        this.connectionConfig = connectionConfig;
    }
    
    public String getTableName() {
        return tableName;
    }
    
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    
    public List<String> getSelectedColumns() {
        return selectedColumns;
    }
    
    public void setSelectedColumns(List<String> selectedColumns) {
        this.selectedColumns = selectedColumns;
    }
    
    public String getSourceFilePath() {
        return sourceFilePath;
    }
    
    public void setSourceFilePath(String sourceFilePath) {
        this.sourceFilePath = sourceFilePath;
    }
    
    public String getTargetFilePath() {
        return targetFilePath;
    }
    
    public void setTargetFilePath(String targetFilePath) {
        this.targetFilePath = targetFilePath;
    }
    
    public String getDelimiter() {
        return delimiter;
    }
    
    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }
}