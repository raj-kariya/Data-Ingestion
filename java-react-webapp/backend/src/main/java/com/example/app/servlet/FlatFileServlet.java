package com.example.app.servlet;

import com.example.app.model.TableInfo;
import com.example.app.service.FlatFileService;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.servlet.ServletException;
import javax.servlet.annotation.MultipartConfig;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;
import java.io.*;
import com.google.gson.JsonObject;

@WebServlet("/api/flatfile/*")
@MultipartConfig(
    fileSizeThreshold = 1024 * 1024,      // 1 MB
    maxFileSize = 1024 * 1024 * 10,       // 10 MB
    maxRequestSize = 1024 * 1024 * 50     // 50 MB
)
public class FlatFileServlet extends HttpServlet {
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final FlatFileService flatFileService = new FlatFileService();
    private final String UPLOAD_DIR = System.getProperty("java.io.tmpdir");
    
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) 
            throws ServletException, IOException {
        
        response.setContentType("application/json");
        PrintWriter out = response.getWriter();
        
        // Get the path info to determine which operation to perform
        String pathInfo = request.getPathInfo();
        
        if (pathInfo == null || pathInfo.equals("/") || pathInfo.equals("/upload")) {
            // Handle file upload
            handleFileUpload(request, response, out);
        } else if (pathInfo.equals("/schema")) {
            // Handle schema request
            handleSchemaRequest(request, response, out);
        } else {
            // Unknown endpoint
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            JsonObject error = new JsonObject();
            error.addProperty("success", false);
            error.addProperty("message", "Unknown endpoint: " + pathInfo);
            out.print(error.toString());
        }
    }

    private void handleFileUpload(HttpServletRequest request, HttpServletResponse response, PrintWriter out) 
            throws ServletException, IOException {
        try {
            // Create a temporary file with a sanitized name
            Part filePart = request.getPart("file");
            String originalFileName = getSubmittedFileName(filePart);
            String fileExtension = originalFileName.substring(originalFileName.lastIndexOf("."));
            
            // Create a temporary file with the original extension
            File tempFile = File.createTempFile("upload_", fileExtension);
            
            // Write the file content
            try (InputStream inputStream = filePart.getInputStream();
                FileOutputStream outputStream = new FileOutputStream(tempFile)) {
                
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, bytesRead);
                }
            }
            
            // Return JSON response with file info
            JsonObject jsonResponse = new JsonObject();
            jsonResponse.addProperty("success", true);
            jsonResponse.addProperty("message", "File uploaded successfully");
            jsonResponse.addProperty("fileName", originalFileName);
            jsonResponse.addProperty("filePath", tempFile.getAbsolutePath());
            
            out.print(jsonResponse.toString());
        } catch (Exception e) {
            // Handle error
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            JsonObject jsonResponse = new JsonObject();
            jsonResponse.addProperty("success", false);
            jsonResponse.addProperty("message", "Error uploading file: " + e.getMessage());
            
            out.print(jsonResponse.toString());
        }
    }

    private void handleSchemaRequest(HttpServletRequest request, HttpServletResponse response, PrintWriter out) 
            throws IOException {
        try {
            // Parse request body as JSON
            BufferedReader reader = request.getReader();
            FileSchemaRequest schemaRequest = objectMapper.readValue(reader, FileSchemaRequest.class);
            
            if (schemaRequest.getFilePath() == null || schemaRequest.getFilePath().isEmpty()) {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                JsonObject error = new JsonObject();
                error.addProperty("success", false);
                error.addProperty("message", "Missing file path");
                out.print(error.toString());
                return;
            }
            
            // Call the FlatFileService to read the schema
            TableInfo tableInfo = flatFileService.readFileSchema(
                schemaRequest.getFilePath(), 
                schemaRequest.getDelimiter()
            );
            
            // Write the response
            String jsonResponse = objectMapper.writeValueAsString(tableInfo);
            out.print(jsonResponse);
            
        } catch (Exception e) {
            e.printStackTrace();
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            JsonObject error = new JsonObject();
            error.addProperty("success", false);
            error.addProperty("message", "Error reading file schema: " + e.getMessage());
            out.print(error.toString());
        }
    }
    
    // Helper method to get the file name from part
    private String getSubmittedFileName(Part part) {
        for (String cd : part.getHeader("content-disposition").split(";")) {
            if (cd.trim().startsWith("filename")) {
                String fileName = cd.substring(cd.indexOf('=') + 1).trim().replace("\"", "");
                return fileName.substring(fileName.lastIndexOf('/') + 1)
                        .substring(fileName.lastIndexOf('\\') + 1);
            }
        }
        return "unknown_filename";
    }
    
    // Helper classes for request/response
    private static class ResponseMessage {
        private String message;
        
        public ResponseMessage(String message) {
            this.message = message;
        }
        
        public String getMessage() {
            return message;
        }
    }
    
    private static class FileUploadResponse {
        private String filePath;
        private String fileName;
        
        public FileUploadResponse(String filePath, String fileName) {
            this.filePath = filePath;
            this.fileName = fileName;
        }
        
        public String getFilePath() {
            return filePath;
        }
        
        public String getFileName() {
            return fileName;
        }
    }
    
    private static class FileSchemaRequest {
        private String filePath;
        private String delimiter;
        
        public String getFilePath() {
            return filePath;
        }
        
        public void setFilePath(String filePath) {
            this.filePath = filePath;
        }
        
        public String getDelimiter() {
            return delimiter != null && !delimiter.isEmpty() ? delimiter : ",";
        }
        
        public void setDelimiter(String delimiter) {
            this.delimiter = delimiter;
        }
    }
}