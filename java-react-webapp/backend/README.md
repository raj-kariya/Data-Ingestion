# Backend Documentation

This is the backend part of the Java-React web application. It is built using Java Servlets and provides APIs for the frontend.

## Project Structure

- **src/main/java/com/example/app**: Contains the main application code.
  - **Main.java**: The entry point of the application.
  - **controller**: Contains the API servlets.
    - **ApiServlet.java**: Handles HTTP requests and responses.
  - **model**: Contains the data models.
    - **Entity.java**: Represents the data structure.
  - **service**: Contains the business logic.
    - **EntityService.java**: Interacts with the data layer.

- **src/main/resources**: Contains configuration files.
  - **config.properties**: Configuration settings for the application.

- **src/test/java/com/example/app**: Contains unit tests for the application.
  - **EntityServiceTest.java**: Tests for the EntityService class.

- **pom.xml**: Maven configuration file that lists dependencies and build settings.

## Getting Started

1. **Clone the repository**:
   ```
   git clone <repository-url>
   ```

2. **Navigate to the backend directory**:
   ```
   cd backend
   ```

3. **Build the project**:
   ```
   mvn clean install
   ```

4. **Run the application**:
   ```
   mvn jetty:run
   ```

5. **Access the API**: The backend will be running on `http://localhost:8080`.

## API Endpoints

- **GET /api/entities**: Retrieve a list of entities.
- **GET /api/entities/{id}**: Retrieve a specific entity by ID.

## Testing

To run the tests, use the following command:
```
mvn test
```

## License

This project is licensed under the MIT License.