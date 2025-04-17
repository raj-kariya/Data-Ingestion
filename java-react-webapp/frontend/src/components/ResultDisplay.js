import React from 'react';

const ResultDisplay = ({ result, status }) => {
    // Map status to display text
    const getStatusText = () => {
        switch (status) {
            case 'idle': return 'Ready';
            case 'connecting': return 'Connecting to ClickHouse...';
            case 'connected': return 'Connected to ClickHouse';
            case 'fetching_tables': return 'Fetching tables...';
            case 'tables_loaded': return 'Tables loaded';
            case 'fetching_schema': return 'Fetching table schema...';
            case 'schema_loaded': return 'Schema loaded';
            case 'file_selected': return 'File selected';
            case 'uploading': return 'Uploading file...';
            case 'uploaded': return 'File uploaded';
            case 'loading_schema': return 'Loading file schema...';
            case 'ingesting': return 'Data ingestion in progress...';
            case 'completed': return 'Operation completed successfully';
            case 'error': return 'Error occurred';
            default: return status;
        }
    };
    
    // Helper for status color
    const getStatusColor = () => {
        if (status === 'error') return 'status-error';
        if (status === 'completed') return 'status-success';
        if (status === 'idle') return 'status-idle';
        return 'status-progress';
    };
    
    return (
        <div className="result-display">
            <div className={`status-area ${getStatusColor()}`}>
                <h3>Status</h3>
                <p>{getStatusText()}</p>
            </div>
            
            {result && (
                <div className={`result-area ${result.success ? 'result-success' : 'result-error'}`}>
                    <h3>Result</h3>
                    {result.success ? (
                        <div>
                            <p className="success-message">{result.message}</p>
                            <p>Records processed: {result.recordsProcessed}</p>
                            <p>Execution time: {result.executionTimeMs}ms</p>
                        </div>
                    ) : (
                        <p className="error-message">{result.message}</p>
                    )}
                </div>
            )}
        </div>
    );
};

export default ResultDisplay;