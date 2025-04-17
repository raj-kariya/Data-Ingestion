import axios from 'axios';

const API_URL = 'http://localhost:8080/api';

// Connection testing
export const testConnection = async (connectionConfig) => {
    try {
        const response = await axios.post(`${API_URL}/clickhouse/connect`, connectionConfig);
        return response.data.message === 'Connected successfully';
    } catch (error) {
        console.error('Connection test failed:', error);
        throw new Error(error.response?.data?.message || 'Connection failed');
    }
};

// Get tables
export const getTables = async (connectionConfig) => {
    try {
        const response = await axios.post(`${API_URL}/clickhouse/tables`, connectionConfig);
        return response.data;
    } catch (error) {
        console.error('Failed to get tables:', error);
        throw new Error(error.response?.data?.message || 'Failed to retrieve tables');
    }
};

// Get table schema
export const getTableSchema = async (connectionConfig, tableName) => {
    try {
        const response = await axios.post(`${API_URL}/clickhouse/schema`, {
            connectionConfig,
            tableName
        });
        return response.data;
    } catch (error) {
        console.error('Failed to get schema:', error);
        throw new Error(error.response?.data?.message || 'Failed to retrieve schema');
    }
};

// Upload file
export const uploadFile = async (formData) => {
    try {
        console.log('Uploading file with formData:', formData);
        const response = await axios.post(`${API_URL}/flatfile/upload`, formData, {
            headers: {
                'Content-Type': 'multipart/form-data'
            }
        });
        console.log('Upload response:', response.data);
        return response.data;
    } catch (error) {
        console.error('Failed to upload file:', error);
        console.error('Error response:', error.response?.data);
        throw new Error(error.response?.data?.message || 'Failed to upload file');
    }
};

export const getFileSchema = async (filePath, delimiter) => {
    try {
        console.log('Getting schema for:', filePath, 'with delimiter:', delimiter);
        const response = await axios.post(`${API_URL}/flatfile/schema`, {
            filePath,
            delimiter
        });
        console.log('Schema response:', response.data);
        return response.data;
    } catch (error) {
        console.error('Failed to get file schema:', error);
        console.error('Error response:', error.response?.data);
        throw new Error(error.response?.data?.message || 'Failed to read file schema');
    }
};

// Ingest data
export const ingestData = async (ingestRequest) => {
    try {
        const response = await axios.post(`${API_URL}/ingest`, ingestRequest);
        console.log('Ingestion response from backend:', response);
        return response.data;
    } catch (error) {
        console.error('Ingestion failed from backend:', error);
        throw new Error(error.response?.data?.message || 'Ingestion failed');
    }
};

export const createTable = async (params) => {
    try {
        const response = await axios.post(`${API_URL}/create-table`, params);
        return response.data;
    } catch (error) {
        console.error('Table creation failed:', error);
        throw new Error(error.response?.data?.message || 'Table creation failed');
    }
};

/**
 * Preview data from a data source
 * @param {Object} previewRequest - The request configuration
 * @returns {Promise<Array>} - A promise that resolves to an array of preview data
 */
export const previewData = async (previewRequest) => {
    try {
      const response = await fetch(`${API_URL}/preview`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(previewRequest),
      });
      
      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Failed to preview data: ${errorText}`);
      }
      
      return await response.json();
    } catch (error) {
      console.error('Error in previewData:', error);
      throw error;
    }
};

export const checkIngestStatus = async (operationId) => {
    try {
        const response = await fetch(`${API_URL}/ingest/status?operationId=${operationId}`, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json'
            }
        });
        
        console.log('Ingest status response backend: ', response);
        if (!response.ok) {
            console.error('Failed to check ingest status:', response.statusText);
            throw new Error(`Status check failed: ${response.statusText}`);
        }
        
        return await response.json();
    } catch (error) {
        console.error('Error checking ingest status backend:', error);
        throw error;
    }
};