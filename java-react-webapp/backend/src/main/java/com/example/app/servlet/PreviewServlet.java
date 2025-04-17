package com.example.app.servlet;

import com.example.app.model.ConnectionConfig;
import com.example.app.service.ClickHouseService;
import com.example.app.service.FlatFileService;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.sql.SQLException;

@WebServlet("/api/preview")
public class PreviewServlet extends HttpServlet {
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final FlatFileService flatFileService = new FlatFileService();
    private final ClickHouseService clickHouseService = new ClickHouseService();
    
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) 
            throws ServletException, IOException {
        
        // Parse the incoming JSON
        BufferedReader reader = req.getReader();
        Map<String, Object> requestMap = objectMapper.readValue(reader, Map.class);
        
        resp.setContentType("application/json");
        PrintWriter out = resp.getWriter();
        
        try {
            String sourceType = (String) requestMap.get("sourceType");
            List<String> selectedColumns = (List<String>) requestMap.get("selectedColumns");
            int maxRows = requestMap.containsKey("maxRows") ? 
                    (Integer) requestMap.get("maxRows") : 100;
            
            List<List<Object>> previewData;
            
            if ("ClickHouse".equals(sourceType)) {
                // Preview ClickHouse data
                ConnectionConfig config = objectMapper.convertValue(
                        requestMap.get("connectionConfig"), ConnectionConfig.class);
                String tableName = (String) requestMap.get("tableName");
                
                // Build query with LIMIT clause
                StringBuilder queryBuilder = new StringBuilder("SELECT ");
                for (int i = 0; i < selectedColumns.size(); i++) {
                    if (i > 0) queryBuilder.append(", ");
                    queryBuilder.append("`").append(selectedColumns.get(i)).append("`");
                }
                
                // Format the table name correctly for ClickHouse
                String database, table;
                if (tableName.contains(".")) {
                    String[] parts = tableName.split("\\.", 2);
                    database = parts[0];
                    table = parts[1];
                } else {
                    database = config.getDatabase();
                    table = tableName;
                }
                
                // Use proper ClickHouse identifier syntax: `database`.`table` instead of `database.table`
                queryBuilder.append(" FROM `").append(database).append("`.`")
                        .append(table).append("`")
                        .append(" LIMIT ").append(maxRows);
                
                try {
                    previewData = clickHouseService.executeQuery(config, queryBuilder.toString());
                } catch (SQLException e) {
                    throw new ServletException("Error querying ClickHouse: " + e.getMessage(), e);
                }
            } else {
                // Preview flat file data
                String filePath = (String) requestMap.get("sourceFilePath");
                String delimiter = (String) requestMap.get("delimiter");
                if (delimiter == null || delimiter.isEmpty()) {
                    delimiter = ",";  // Default delimiter
                }
                
                previewData = flatFileService.previewFileData(filePath, delimiter, selectedColumns, maxRows);
            }
            
            out.print(objectMapper.writeValueAsString(previewData));
            
        } catch (Exception e) {
            resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            out.print(objectMapper.writeValueAsString(Map.of(
                "error", true,
                "message", "Error previewing data: " + e.getMessage()
            )));
        }
    }
}