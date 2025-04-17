package com.example.app.servlet;

import com.example.app.model.IngestRequest;
import com.example.app.model.IngestResult;
import com.example.app.service.ClickHouseService;
import com.example.app.service.FlatFileService;
import com.example.app.service.IngestService;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@WebServlet("/api/ingest/*")
public class IngestServlet extends HttpServlet {
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final IngestService ingestService = new IngestService(
            new ClickHouseService(), 
            new FlatFileService());
    
    private final ExecutorService executorService = Executors.newFixedThreadPool(5);

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) 
            throws ServletException, IOException {
        
        // Log the request path and parameters
        System.out.println("GET request to: " + req.getRequestURI());
        System.out.println("Query parameters: " + req.getQueryString());

        String operationId = req.getParameter("operationId");
        resp.setContentType("application/json");
        
        if (operationId == null || operationId.isEmpty()) {
            System.err.println("Missing required parameter: operationId");
            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            resp.getWriter().write(objectMapper.writeValueAsString(
                    new ResponseMessage("Missing required parameter: operationId")));
            return;
        }
        
        try {
            IngestResult status = ingestService.getOperationStatus(operationId);
            
            if (status == null) {
                System.err.println("No operation found with ID: " + operationId);
                resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
                resp.getWriter().write(objectMapper.writeValueAsString(
                        new ResponseMessage("No operation found with ID: " + operationId)));
                return;
            }
            System.out.println("Operation status: " + status.getStatus());
            resp.getWriter().write(objectMapper.writeValueAsString(status));
        } catch (Exception e) {
            System.err.println("Error retrieving operation status: " + e.getMessage());
            e.printStackTrace();
            resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            resp.getWriter().write(objectMapper.writeValueAsString(
                    new ResponseMessage("Error: " + e.getMessage())));
        }
    }
    
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
            final IngestRequest request = objectMapper.readValue(body.toString(), IngestRequest.class);
            
            // Create a result object with operation ID but no data yet
            final IngestResult initialResult = new IngestResult();
            initialResult.setStatus("running");
            initialResult.setMessage("Operation started");
            
            // Start tracking this operation
            ingestService.trackOperation(initialResult);
            
            // Return the operation ID immediately
            resp.getWriter().write(objectMapper.writeValueAsString(initialResult));
            
            // Process in background
            executorService.submit(() -> {
                try {
                    IngestResult finalResult;
                    
                    if ("ClickHouse".equals(request.getSourceType())) {
                        // ClickHouse to Flat File
                        finalResult = ingestService.streamFromClickHouseToFile(
                                request.getConnectionConfig(),
                                request.getTableName(),
                                request.getSelectedColumns(),
                                request.getTargetFilePath(),
                                initialResult.getOperationId());
                    } else if ("FlatFile".equals(request.getSourceType())) {
                        // Flat File to ClickHouse
                        finalResult = ingestService.streamFromFileToClickHouse(
                                request.getSourceFilePath(),
                                request.getConnectionConfig(),
                                request.getTableName(),
                                request.getSelectedColumns(),
                                initialResult.getOperationId());
                    } else {
                        System.err.println("Invalid source type: " + request.getSourceType());
                        initialResult.setSuccess(false);
                        initialResult.setStatus("error");
                        initialResult.setMessage("Invalid source type");
                        return;
                    }
                    
                    // Update tracked operation with final results
                    ingestService.updateOperation(finalResult);
                } catch (Exception e) {
                    System.err.println("Error processing request: " + e.getMessage());
                    e.printStackTrace();
                    // Update with error
                    initialResult.setSuccess(false);
                    initialResult.setStatus("error");
                    initialResult.setMessage("Error: " + e.getMessage());
                    ingestService.updateOperation(initialResult);
                }
            });
            
        } catch (Exception e) {
            System.err.println("Error parsing request: " + e.getMessage());
            e.printStackTrace();
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
}