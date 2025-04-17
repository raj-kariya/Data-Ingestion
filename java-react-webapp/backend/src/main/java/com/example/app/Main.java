package com.example.app;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;

import java.io.File;

public class Main {
    public static void main(String[] args) throws Exception {
        // Create and configure a Jetty server instance
        Server server = new Server(8080);
        
        // Define webapp directory
        String webappDir = "src/main/webapp";
        File webDir = new File(webappDir);
        if (!webDir.exists()) {
            webDir.mkdirs();
        }
        
        // Configure web application context
        WebAppContext context = new WebAppContext();
        context.setContextPath("/");
        context.setResourceBase(webDir.getAbsolutePath());
        context.setClassLoader(Thread.currentThread().getContextClassLoader());
        
        // Add classes from target directory to web application
        File classesDir = new File("target/classes");
        context.setExtraClasspath(classesDir.getAbsolutePath());
        
        // Set the context as handler for the server
        server.setHandler(context);
        
        // Start the server
        server.start();
        System.out.println("Server started on port 8080. Press Ctrl+C to stop.");
        server.join();
    }
}