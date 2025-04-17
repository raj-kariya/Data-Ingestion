/* eslint-disable react/jsx-no-comment-textnodes */
import React, { useState } from 'react';
import ClickHouseForm from './ClickHouseForm';
import ColumnSelector from './ColumnSelector';
import IngestControls from './IngestControls';
import ResultDisplay from './ResultDisplay';
import { uploadFile, getFileSchema } from '../services/api';

const FileUploader = ({ direction }) => {
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
        setFile(selectedFile);
        setStatus('file_selected');
    };
    
    const handleDelimiterChange = (e) => {
        setDelimiter(e.target.value);
    };
    
    const handleUpload = async () => {
        if (!file) return;
        
        setStatus('uploading');
        
        try {
            const formData = new FormData();
            formData.append('file', file);
            formData.append('delimiter', delimiter);
            const response = await uploadFile(formData);
            console.log('Upload response:', response); // Debug the full response

          if (response && response.filePath) {
              setFilePath(response.filePath);
              console.log('Setting filePath to:', response.filePath);
              setStatus('uploaded');
              
              // Automatically load schema
              await handleLoadSchema(response.filePath);
          } else {
            console.error('Invalid response format:', response);
              throw new Error('Invalid response format from server');
          }
        } catch (error) {
          console.error('Upload error:', error);

            setStatus('error');
            setResult({ success: false, message: `Error uploading file: ${error.message}` });
        }
    };
    
    const handleLoadSchema = async (path) => {
        setStatus('loading_schema');
        
        try {
            const schema = await getFileSchema(path || filePath, delimiter);
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
    
    const handleConnectionConfig = (connConfig, table) => {
        setConnection(connConfig);
        setTableName(table);
        setShowClickHouseForm(false);
    };
    
    const handleStartIngest = async (parameters) => {
        setStatus('ingesting');
        try {
            // This will be handled by IngestControls component
            setResult(parameters.result);
            setStatus(parameters.result.success ? 'completed' : 'error');
        } catch (error) {
            setStatus('error');
            setResult({ success: false, message: `Error during ingestion: ${error.message}` });
        }
    };
    
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
            
            {/* Add safe checks for columns */}
            {columns && columns.length > 0 && (
                <ColumnSelector 
                    columns={columns} 
                    selectedColumns={selectedColumns || []}
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
                        <div className="connection-summary">
                            <p><strong>Host:</strong> {connection.host}:{connection.port}</p>
                            <p><strong>Database:</strong> {connection.database}</p>
                            <p><strong>Table:</strong> {tableName}</p>
                            <button onClick={() => setShowClickHouseForm(true)}>Change</button>
                        </div>
                    )}
                    
                    {showClickHouseForm && (
                        <div className="modal">
                            <div className="modal-content">
                                <span className="close" onClick={() => setShowClickHouseForm(false)}>&times;</span>
                                <ClickHouseForm 
                                    direction="import" 
                                    onConnectionConfig={handleConnectionConfig} 
                                />
                            </div>
                        </div>
                    )}
                </div>
            )}

            {console.log('FileUploader - Render check:', {
                hasConnection: !!connection,
                tableName: tableName || 'MISSING',
                selectedColumnsLength: selectedColumns?.length || 0,
                filePath: filePath || 'MISSING',
                willRenderIngestControls: !!(connection && tableName && selectedColumns?.length > 0 && filePath)
            })}
            
            {/* Render IngestControls with more detailed conditions */}
            {connection && tableName && selectedColumns && selectedColumns.length > 0 ? (
                filePath ? (
                    <IngestControls 
                        direction="import"
                        connection={connection}
                        tableName={tableName}
                        selectedColumns={selectedColumns}
                        filePath={filePath}
                        onStartIngest={handleStartIngest}
                        status={status}
                    />
                ) : (
                    <div className="missing-filepath-warning">
                        <p>File path is missing. Please ensure a file has been uploaded successfully.</p>
                        <p>Current status: {status}</p>
                    </div>
                )
            ) : (
                <div className="missing-data-warning">
                    <p>Please complete all required information:</p>
                    <ul>
                        {!connection && <li>Configure ClickHouse connection</li>}
                        {!tableName && <li>Specify a table name</li>}
                        {(!selectedColumns || selectedColumns.length === 0) && <li>Select at least one column</li>}
                        {!filePath && <li>Upload a file</li>}
                    </ul>
                </div>
            )}
            
            <ResultDisplay result={result} status={status} />
        </div>
    );
};

export default FileUploader;