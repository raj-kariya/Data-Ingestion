import React, { useState } from 'react';
import ClickHouseConnectionForm from './ClickHouseConnectionForm';
import ColumnSelector from './ColumnSelector';
import IngestControls from './IngestControls';
import ResultDisplay from './ResultDisplay';
import { uploadFile, getFileSchema } from '../services/api';

const FileToClickHouse = () => {
    const [file, setFile] = useState(null);
    const [filePath, setFilePath] = useState('');
    const [delimiter, setDelimiter] = useState(',');
    const [columns, setColumns] = useState([]);
    const [selectedColumns, setSelectedColumns] = useState([]);
    const [showClickHouseForm, setShowClickHouseForm] = useState(false);
    const [status, setStatus] = useState('idle');
    const [result, setResult] = useState(null);
    const [connection, setConnection] = useState(null);
    const [tableName, setTableName] = useState('');
    
    const handleFileChange = (e) => {
        const selectedFile = e.target.files[0];
        if (selectedFile) {
            setFile(selectedFile);
            // Set the filename immediately for display purposes
            setFilePath(selectedFile.name);
            setStatus('file_selected');
            console.log("File selected:", selectedFile.name);
        }
    };
    
    const handleDelimiterChange = (e) => {
        setDelimiter(e.target.value);
    };
    
    const handleUpload = async () => {
        if (!file) {
            alert('Please select a file first');
            return;
        }
        
        setStatus('uploading');
        
        try {
            const formData = new FormData();
            formData.append('file', file);
            formData.append('delimiter', delimiter);
            
            console.log('Uploading file:', file.name, 'with delimiter:', delimiter);
            
            const response = await uploadFile(formData);
            console.log('Upload response:', response);
            
            if (response && response.filePath) {
                setFilePath(response.filePath);
                setStatus('uploaded');
                
                // Automatically load schema
                await handleLoadSchema(response.filePath);
            } else {
                console.error('Invalid response format:', response);
                throw new Error('Invalid response format from server');
            }
        } catch (error) {
            console.error('File upload error:', error);
            setStatus('error');
            setResult({ success: false, message: `Error uploading file: ${error.message}` });
        }
    };
    
    const handleLoadSchema = async (path) => {
        setStatus('loading_schema');
        
        try {
            const schema = await getFileSchema(path || filePath, delimiter);
            console.log('Schema loaded:', schema);
            
            if (schema && schema.columns) {
                setColumns(schema.columns);
                // Safely get selected columns
                const selectedCols = schema.columns
                    .filter(col => col && col.selected)
                    .map(col => col.name);
                setSelectedColumns(selectedCols);
                setStatus('schema_loaded');
            } else {
                throw new Error('Invalid schema format from server');
            }
        } catch (error) {
            console.error('Schema loading error:', error);
            setStatus('error');
            setResult({ success: false, message: `Error loading schema: ${error.message}` });
        }
    };
    
    const handleColumnSelect = (columnName, isSelected) => {
        if (isSelected) {
            setSelectedColumns(prev => [...prev, columnName]);
        } else {
            setSelectedColumns(prev => prev.filter(col => col !== columnName));
        }
    };
    
    const handleConnectionSuccess = (connConfig) => {
        setConnection(connConfig);
        setShowClickHouseForm(false);
    };
    
    const handleStartIngest = async (parameters) => {
        console.log('handleStartIngest called with parameters:', parameters);
        
        // First, always set status to ingesting when the process starts
        if (parameters.status === 'ingesting') {
            console.log('Setting status to ingesting - operation started');
            setStatus('ingesting');
            return; // Return early - we'll update result later when process completes
        }
        
        try {
            // Handle result when process completes or errors
            if (parameters.result) {
                console.log('Ingest operation result:', parameters.result);
                console.log('Status from parameters:', parameters.status);
                console.log('Success from result:', parameters.result.success);
                console.log('Message from result:', parameters.result.message || 'No message');
                
                // Store the result
                setResult(parameters.result);
                
                // Use the explicit status if available, otherwise determine from success
                if (parameters.status === 'completed' || parameters.status === 'error') {
                    console.log(`Setting explicit status to: ${parameters.status}`);
                    setStatus(parameters.status);
                } else if (parameters.result.status === 'completed' || parameters.result.status === 'error') {
                    console.log(`Setting status from result.status: ${parameters.result.status}`);
                    setStatus(parameters.result.status);
                } else {
                    // Fall back to success flag if status not explicitly provided
                    const newStatus = parameters.result.success ? 'completed' : 'error';
                    console.log(`Setting derived status based on success flag: ${newStatus}`);
                    setStatus(newStatus);
                }
            } else {
                console.warn('handleStartIngest called with no result object');
            }
        } catch (error) {
            console.error('Error in handleStartIngest:', error);
            setStatus('error');
            setResult({ 
                success: false, 
                message: `Error during ingestion: ${error.message}`,
                error: error.toString() 
            });
        } finally {
            // Add a debug log of final state after update
            console.log("FileToClickHouse state after ingest update:", {
                connection,
                tableName,
                selectedColumns,
                filePath,
                status,
                result: parameters.result
            });
        }
    };

    // Update the handleTableNameChange function

    const handleTableNameChange = (e) => {
        // Strip any database prefix if the user inputs it
        let tableName = e.target.value;
        if (tableName.includes('.')) {
            // Just take the part after the last dot
            tableName = tableName.split('.').pop();
        }
        setTableName(tableName);
    };
    
    // Debug log
    console.log("FileToClickHouse render state:", {
        connection,
        tableName,
        selectedColumns,
        filePath,
        status,
        result: result ? {
            success: result.success,
            status: result.status,
            message: result.message,
            recordsProcessed: result.recordsProcessed,
            error: result.error || 'none'
        } : 'none'
    });
    
    return (
        <div className="file-uploader">
            <h2>File to ClickHouse</h2>
            
            <div className="upload-section">
                <h3>Upload File</h3>
                <div className="form-group">
                    <label>Select File:</label>
                    <input type="file" onChange={handleFileChange} />
                </div>
                <div className="form-group">
                    <label>Delimiter:</label>
                    <select value={delimiter} onChange={handleDelimiterChange}>
                        <option value=",">Comma (,)</option>
                        <option value=";">Semicolon (;)</option>
                        <option value="\t">Tab</option>
                        <option value="|">Pipe (|)</option>
                    </select>
                </div>
                <button 
                    onClick={handleUpload} 
                    disabled={!file || status === 'uploading'}
                >
                    {status === 'uploading' ? 'Uploading...' : 'Upload File'}
                </button>
            </div>
            
            {columns && columns.length > 0 && (
                <ColumnSelector 
                    columns={columns} 
                    selectedColumns={selectedColumns}
                    onColumnSelect={handleColumnSelect} 
                />
            )}
            
            {columns && columns.length > 0 && selectedColumns && selectedColumns.length > 0 && (
                <div className="clickhouse-target-section">
                    <h3>ClickHouse Target</h3>
                    {!connection ? (
                        <button onClick={() => setShowClickHouseForm(true)}>
                            Configure ClickHouse Connection
                        </button>
                    ) : (
                        <>
                            <div className="connection-summary">
                                <p><strong>Host:</strong> {connection.host}:{connection.port}</p>
                                <p><strong>Database:</strong> {connection.database}</p>
                                <button onClick={() => setShowClickHouseForm(true)}>Change Connection</button>
                            </div>
                            
                            <div className="form-group">
                                <label>Target Table Name:</label>
                                <input 
                                    type="text" 
                                    value={tableName} 
                                    onChange={handleTableNameChange} 
                                    placeholder="Enter table name" 
                                />
                            </div>
                        </>
                    )}
                    
                    {showClickHouseForm && (
                        <div className="modal">
                            <div className="modal-content">
                                <span className="close" onClick={() => setShowClickHouseForm(false)}>&times;</span>
                                <ClickHouseConnectionForm onConnectionSuccess={handleConnectionSuccess} />
                            </div>
                        </div>
                    )}
                </div>
            )}
            
            {connection && tableName && selectedColumns && selectedColumns.length > 0 && filePath && (
                <IngestControls 
                    direction="import"
                    connection={connection}
                    tableName={tableName}
                    selectedColumns={selectedColumns}
                    filePath={filePath}
                    onStartIngest={handleStartIngest}
                    status={status}
                />
            )}
            
            <ResultDisplay result={result} status={status} />
        </div>
    );
};

export default FileToClickHouse;