import React, { useState } from 'react';
import ClickHouseConnectionForm from './ClickHouseConnectionForm';
import ColumnSelector from './ColumnSelector';
import IngestControls from './IngestControls';
import ResultDisplay from './ResultDisplay';
import { getTables, getTableSchema } from '../services/api';

const ClickHouseForm = ({ direction }) => {
    const [connection, setConnection] = useState(null);
    const [tableName, setTableName] = useState('');
    const [tables, setTables] = useState([]);
    const [columns, setColumns] = useState([]);
    const [selectedColumns, setSelectedColumns] = useState([]);
    const [status, setStatus] = useState('idle');
    const [result, setResult] = useState(null);
    const [filePath, setFilePath] = useState('');
    
    const handleConnectionSuccess = async (connectionConfig) => {
        setConnection(connectionConfig);
        setStatus('connected');
        await loadTables(connectionConfig);
    };
    
    const loadTables = async (conn) => {
        setStatus('fetching_tables');
        try {
            const tablesList = await getTables(conn || connection);
            setTables(tablesList);
            setStatus('tables_loaded');
        } catch (error) {
            setStatus('error');
            setResult({ success: false, message: `Error loading tables: ${error.message}` });
        }
    };
    
    const handleTableSelect = async (e) => {
        const selectedTable = e.target.value;
        setTableName(selectedTable);
        setStatus('fetching_schema');
        
        try {
            const schema = await getTableSchema(connection, selectedTable);
            setColumns(schema.columns);
            setSelectedColumns(schema.columns.filter(col => col.selected).map(col => col.name));
            setStatus('schema_loaded');
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
    
    const handleFilePathChange = (e) => {
        setFilePath(e.target.value);
    };
    
    const handleStartIngest = async (parameters) => {
        console.log('handleStartIngest called with parameters:', parameters);
        
        if (parameters.status === 'ingesting') {
            console.log('Setting status to ingesting');
            setStatus('ingesting');
            return; // Return early - we'll update result later when process completes
        }
        
        try {
            // Handle the result when process completes or errors
            if (parameters.result) {
                console.log('Ingest operation result:', parameters.result);
                
                // Store the result
                setResult(parameters.result);
                
                // Determine the status based on the parameters or result
                if (parameters.status === 'completed' || parameters.status === 'error') {
                    console.log(`Setting status from parameters: ${parameters.status}`);
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
                setStatus('error');
                setResult({ 
                    success: false, 
                    message: 'No result received from ingestion process' 
                });
            }
        } catch (error) {
            console.error('Error in handleStartIngest:', error);
            setStatus('error');
            setResult({ 
                success: false, 
                message: `Error during ingestion: ${error.message}`,
                error: error.toString()
            });
        }
    };
    
    return (
        <div className="clickhouse-form">
            <h2>{direction === 'export' ? 'ClickHouse to File' : 'ClickHouse Connection'}</h2>
            
            {!connection && (
                <ClickHouseConnectionForm onConnectionSuccess={handleConnectionSuccess} />
            )}
            
            {connection && (
                <>
                    <div className="connection-summary">
                        <h3>Connection</h3>
                        <p><strong>Host:</strong> {connection.host}:{connection.port}</p>
                        <p><strong>Database:</strong> {connection.database}</p>
                        <p><strong>User:</strong> {connection.user}</p>
                        <button onClick={() => setConnection(null)}>Change Connection</button>
                    </div>
                    
                    <div className="table-selection">
                        <h3>Table Selection</h3>
                        <select value={tableName} onChange={handleTableSelect}>
                            <option value="">Select a table</option>
                            {tables.map(table => (
                                <option key={table} value={table}>{table}</option>
                            ))}
                        </select>
                    </div>
                </>
            )}
            
            {columns.length > 0 && (
                <ColumnSelector 
                    columns={columns} 
                    selectedColumns={selectedColumns}
                    onColumnSelect={handleColumnSelect} 
                />
            )}
            
            {direction === 'export' && tableName && selectedColumns.length > 0 && (
                <div className="file-path-form">
                    <h3>Output File Path</h3>
                    <div className="form-group">
                        <label>Save to:</label>
                        <input 
                            type="text" 
                            value={filePath} 
                            onChange={handleFilePathChange} 
                            placeholder="/path/to/output.csv" 
                        />
                    </div>
                </div>
            )}
            
            {connection && tableName && selectedColumns.length > 0 && (
                <IngestControls 
                    direction={direction}
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

export default ClickHouseForm;
