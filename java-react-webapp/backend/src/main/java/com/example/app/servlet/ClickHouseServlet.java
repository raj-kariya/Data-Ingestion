package com.example.app.servlet;

import com.example.app.model.ConnectionConfig;
import com.example.app.model.TableInfo;
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

@WebServlet("/api/clickhouse/*")
public class ClickHouseServlet extends HttpServlet {
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ClickHouseService clickHouseService = new ClickHouseService();
    
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) 
            throws ServletException, IOException {
        
        // Get the action from path info
        String pathInfo = req.getPathInfo();
        if (pathInfo == null || pathInfo.equals("/")) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "No action specified");
            return;
        }
        
        // Parse the incoming JSON
        BufferedReader reader = req.getReader();
        StringBuilder body = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            body.append(line);
        }
        
        resp.setContentType("application/json");
        
        try {
            if (pathInfo.equals("/connect")) {
                ConnectionConfig config = objectMapper.readValue(body.toString(), ConnectionConfig.class);
                boolean connected = clickHouseService.testConnection(config);
                
                if (connected) {
                    resp.getWriter().write(objectMapper.writeValueAsString(
                            new ResponseMessage("Connected successfully")));
                } else {
                    resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                    resp.getWriter().write(objectMapper.writeValueAsString(
                            new ResponseMessage("Failed to connect")));
                }
            } else if (pathInfo.equals("/tables")) {
                ConnectionConfig config = objectMapper.readValue(body.toString(), ConnectionConfig.class);
                List<String> tables = clickHouseService.getTables(config);
                resp.getWriter().write(objectMapper.writeValueAsString(tables));
            } else if (pathInfo.equals("/schema")) {
                // Parse the request for table and connection details
                TableRequest tableRequest = objectMapper.readValue(body.toString(), TableRequest.class);
                TableInfo tableInfo = clickHouseService.getTableSchema(
                        tableRequest.getConnectionConfig(), 
                        tableRequest.getTableName());
                
                resp.getWriter().write(objectMapper.writeValueAsString(tableInfo));
            } else {
                resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Unknown action");
            }
        } catch (Exception e) {
            resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            resp.getWriter().write(objectMapper.writeValueAsString(
                    new ResponseMessage("Error: " + e.getMessage())));
        }
    }
    
    // Simple helper classes for request/response
    private static class ResponseMessage {
        private String message;
        
        public ResponseMessage(String message) {
            this.message = message;
        }
        
        public String getMessage() {
            return message;
        }
    }
    
    private static class TableRequest {
        private ConnectionConfig connectionConfig;
        private String tableName;
        
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
    }
}