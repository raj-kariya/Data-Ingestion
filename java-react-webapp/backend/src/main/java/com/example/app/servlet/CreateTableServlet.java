package com.example.app.servlet;

import com.example.app.model.ConnectionConfig;
import com.example.app.service.ClickHouseService;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@WebServlet("/api/create-table")
public class CreateTableServlet extends HttpServlet {
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ClickHouseService clickHouseService = new ClickHouseService();
    
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) 
            throws ServletException, IOException {
        
        // Parse the incoming JSON
        BufferedReader reader = req.getReader();
        StringBuilder body = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            body.append(line);
        }
        
        resp.setContentType("application/json");
        
        try {
            CreateTableRequest createRequest = objectMapper.readValue(body.toString(), CreateTableRequest.class);
            
            // Create the table
            boolean success = clickHouseService.createTable(
                    createRequest.getConnection(),
                    createRequest.getTableName(),
                    createRequest.getColumns(),
                    createRequest.getSourceFilePath());
            
            if (success) {
                resp.getWriter().write(objectMapper.writeValueAsString(
                        new ResponseMessage("Table created successfully")));
            } else {
                resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                resp.getWriter().write(objectMapper.writeValueAsString(
                        new ResponseMessage("Failed to create table")));
            }
        } catch (Exception e) {
            resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            resp.getWriter().write(objectMapper.writeValueAsString(
                    new ResponseMessage("Error: " + e.getMessage())));
        }
    }
    
    private static class ResponseMessage {
        private String message;
        
        public ResponseMessage(String message) {
            this.message = message;
        }
        
        public String getMessage() {
            return message;
        }
    }
    
    private static class CreateTableRequest {
        private ConnectionConfig connection;
        private String tableName;
        private List<String> columns;
        private String sourceFilePath;
        
        public ConnectionConfig getConnection() {
            return connection;
        }
        
        public void setConnection(ConnectionConfig connection) {
            this.connection = connection;
        }
        
        public String getTableName() {
            return tableName;
        }
        
        public void setTableName(String tableName) {
            this.tableName = tableName;
        }
        
        public List<String> getColumns() {
            return columns;
        }
        
        public void setColumns(List<String> columns) {
            this.columns = columns;
        }
        
        public String getSourceFilePath() {
            return sourceFilePath;
        }
        
        public void setSourceFilePath(String sourceFilePath) {
            this.sourceFilePath = sourceFilePath;
        }
    }
}