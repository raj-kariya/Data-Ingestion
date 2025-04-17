import React, { useState, useEffect, useRef } from 'react';
import { ingestData, createTable, previewData, checkIngestStatus } from '../services/api';

const IngestControls = ({ 
    direction, 
    connection, 
    tableName, 
    selectedColumns, 
    filePath, 
    onStartIngest,
    status
}) => {
    const [loading, setLoading] = useState(false);
    const [displayFilePath, setDisplayFilePath] = useState('');
    const [createTableFirst, setCreateTableFirst] = useState(true);
    const [previewDataRows, setPreviewDataRows] = useState(null);
    const [showPreview, setShowPreview] = useState(false);
    // Add progress state
    const [progress, setProgress] = useState(0);
    const [showProgress, setShowProgress] = useState(false)
    const [operationId, setOperationId] = useState(null);
    const [progressStats, setProgressStats] = useState(null);
    // Ensure the file path is formatted appropriately for display
    useEffect(() => {
        if (filePath) {
            // Extract just the filename if it's a full path
            const pathParts = filePath.split(/[\/\\]/);
            const fileName = pathParts[pathParts.length - 1];
            setDisplayFilePath(fileName || filePath);
        } else {
            setDisplayFilePath('(no file selected)');
        }
    }, [filePath]);

    // Use a ref to track if component is mounted (prevents setting state after unmount)
    const isMounted = useRef(true);
    
    useEffect(() => {
        return () => {
            isMounted.current = false;
        };
    }, []);

    // Add a ref to track poll error count
    const pollErrorCount = useRef(0);

    // Reset poll error count when operation changes
    useEffect(() => {
        pollErrorCount.current = 0;
    }, [operationId]);

    // Replace the entire useEffect for polling with this updated version:
    useEffect(() => {
        let pollInterval;
        
        if (status === 'ingesting' && operationId) {
            setShowProgress(true);
            console.log(`Starting progress polling for operation: ${operationId}`); // Add logging
            
            // Poll every second to get progress updates
            pollInterval = setInterval(async () => {
                try {
                    // Get status from backend using the operation ID
                    console.log(`Polling status for operation: ${operationId}`); // Add logging
                    const statusResult = await checkIngestStatus(operationId);
                    console.log('Polling result:', statusResult); // Add logging

                    if (!isMounted.current) return;
                    
                    if (statusResult) {
                        console.log(`Poll result: Status=${statusResult.status}, Records=${statusResult.recordsProcessed}/${statusResult.totalRecords}`);
                        
                        // CRITICAL FIX: Check for terminal states FIRST, before updating progress
                        if (statusResult.status === 'completed') {
                            console.log('Operation completed, setting progress to 100%');
                            setProgress(100);
                            onStartIngest({ result: statusResult, status: 'completed' }); // Notify parent
                            clearInterval(pollInterval);
                            return; // Exit early to avoid further state updates
                        } else if (statusResult.status === 'error') {
                            console.error('Operation error:', statusResult.message);
                            // Keep progress where it is but mark as error
                            console.error('Ingestion error:', statusResult.message);
                            onStartIngest({ result: statusResult, status: 'error' }); // Notify parent
                            clearInterval(pollInterval);
                            return; // Exit early to avoid further state updates
                        }
                        
                        // Only update progress for ongoing operations
                        const stats = {
                            processed: statusResult.recordsProcessed || 0,
                            total: statusResult.totalRecords || statusResult.estimatedTotal || 0,
                            rate: statusResult.recordsPerSecond || 0
                        };
                        
                        setProgressStats(stats);
                        
                        // Calculate progress percentage
                        if (stats.total > 0) {
                            setProgress(Math.min(Math.round((stats.processed / stats.total) * 100), 99));
                        } else {
                            // More conservative progress simulation that never reaches 100%
                            setProgress(prev => {
                                if (prev < 50) return prev + 2;
                                if (prev < 80) return prev + 1;
                                return Math.min(prev + 0.5, 95); // Cap at 95% for simulated progress
                            });
                        }
                    } else {
                        console.warn(`Poll returned no data for operation: ${operationId}`);
                        pollErrorCount.current++;
                    }
                } catch (error) {
                    console.error('Error polling for progress:', error);
                    
                    // After several consecutive failures, assume something went wrong
                    pollErrorCount.current++;
                    if (pollErrorCount.current > 5) {
                        console.error('Too many polling errors, assuming operation failed');
                        onStartIngest({ 
                            result: { 
                                success: false, 
                                status: 'error',
                                message: `Lost connection to ingestion process: ${error.message}` 
                            },
                            status: 'error'
                        });
                        clearInterval(pollInterval);
                    }
                }
            }, 1000);
        } else if (status === 'completed') {
            console.log('Ingestion completed, stopping progress animation');
            // When completed, ensure progress is 100%
            setProgress(100);
            
            // After a short delay, hide the progress bar
            setTimeout(() => {
                if (isMounted.current) {
                    setShowProgress(false);
                }
            }, 1500);
        } else if (status === 'error') {
            console.error('Ingestion error, stopping progress animation');

            // On error, stop any progress animation
            if (pollInterval) {
                clearInterval(pollInterval);
            }
        }

        return () => {
            if (pollInterval) clearInterval(pollInterval);
        };
    }, [status, operationId, onStartIngest]);
    
    const handlePreview = async () => {
        if (!connection || !tableName || !selectedColumns || selectedColumns.length === 0 || !filePath) {
            alert('Please fill in all required fields.');
            return;
        }
        
        setLoading(true);
        
        try {
            // Make sure the tableName doesn't include the database name
            const cleanTableName = tableName.includes('.') 
                ? tableName.split('.').pop() 
                : tableName;
            
            const previewRequest = {
                sourceType: direction === 'export' ? 'ClickHouse' : 'FlatFile',
                connectionConfig: connection,
                tableName: cleanTableName,
                selectedColumns,
                delimiter: ',',
                maxRows: 100 // Preview only first 100 rows
            };
            
            if (direction === 'export') {
                // For ClickHouse to File preview
            } else {
                // For File to ClickHouse preview
                previewRequest.sourceFilePath = filePath;
            }
            
            console.log('Sending preview request:', previewRequest);
            const previewResult = await previewData(previewRequest);
            console.log('Preview result:', previewResult);
            
            setPreviewDataRows(previewResult);
            setShowPreview(true);
        } catch (error) {
            console.error('Preview error:', error);
            alert(`Error getting preview: ${error.message}`);
        } finally {
            setLoading(false);
        }
    };    

    const closePreview = () => {
        setShowPreview(false);
    };


    const handleStartIngest = async () => {
        // Validate required fields
        if (!connection || !tableName || !selectedColumns || selectedColumns.length === 0 || !filePath) {
            alert('Please fill in all required fields.');
            return;
        }
        
        setLoading(true);
        setShowProgress(true);
        setProgress(0);
        setProgressStats(null);
        setOperationId(null);

        try {
            // Make sure the tableName doesn't include the database name
            // Strip 'default.' or any other database prefix if present
            const cleanTableName = tableName.includes('.') 
                ? tableName.split('.').pop() 
                : tableName;
            
            // If this is a file to ClickHouse import and we need to create the table first
            if (direction === 'import' && createTableFirst) {
                try {
                    console.log('Creating table:', cleanTableName);
                    await createTable({
                        connection,
                        tableName: cleanTableName,
                        columns: selectedColumns,
                        sourceFilePath: filePath
                    });
                    console.log('Table created successfully');
                    setProgress(10); // Increment progress after table creation
                } catch (createError) {
                    console.warn('Table creation failed, continuing with import:', createError);
                    // Continue with import even if table creation fails
                    // This allows importing to existing tables
                }
            }
            
            const ingestRequest = {
                sourceType: direction === 'export' ? 'ClickHouse' : 'FlatFile',
                connectionConfig: connection,
                tableName: cleanTableName, // Use clean table name without database prefix
                selectedColumns,
                delimiter: ',' // Add default delimiter
            };
            
            if (direction === 'export') {
                ingestRequest.targetFilePath = filePath;
            } else {
                ingestRequest.sourceFilePath = filePath;
            }
            
            console.log('Sending ingestion request:', ingestRequest);
            const result = await ingestData(ingestRequest);
            console.log('Ingestion result:', result);

            // Store the operation ID for progress polling
            if (result && result.operationId) {
                console.log(`Setting operation ID: ${result.operationId}`);
                setOperationId(result.operationId);

                onStartIngest({ result, status: 'ingesting' });
            }
            else {
                console.warn('No operation ID returned from ingest API');
                // If we don't have an operation ID, create a fallback progress mechanism
                simulateProgressWithoutOperationId(result);

                onStartIngest({ result, status: 'simulating' });
            }

        } catch (error) {
            console.error('Ingestion error:', error);
            onStartIngest({ 
                result: { 
                    success: false, 
                    message: `Failed to ${direction === 'export' ? 'export' : 'import'} data: ${error.message}` 
                } 
            });
        } finally {
            setLoading(false);
        }
    };

    // Add a fallback function for when operationId is not available
    const simulateProgressWithoutOperationId = (result) => {
        // Create artificial progress for when we don't have operationId
        let simProgress = 0;
        const interval = setInterval(() => {
            simProgress += 5;
            setProgress(Math.min(simProgress, 90)); // Never go to 100% until complete
            
            if (simProgress >= 90 || status === 'completed' || status === 'error') {
                clearInterval(interval);
                
                if (status === 'completed') {
                    setProgress(100);
                }
            }
        }, 300);
        
        // Clean up on unmount
        return () => clearInterval(interval);
    };

    const isFormComplete = connection && tableName && selectedColumns && selectedColumns.length > 0 && filePath;
    const canStartIngest = isFormComplete && !loading && status !== 'ingesting';
    
    return (
        <div className="ingest-controls">
            <h3>Ingestion Controls</h3>
            
            {direction === 'import' && (
                <div className="form-group">
                    <div className="checkbox-container">
                        <input 
                            id="create-table-checkbox"
                            type="checkbox" 
                            checked={createTableFirst} 
                            onChange={(e) => setCreateTableFirst(e.target.checked)} 
                        />
                        <label htmlFor="create-table-checkbox">
                            Create table if it doesn't exist
                        </label>
                    </div>
                </div>
            )}
            {/* Add progress bar */}
            {showProgress && (
                <div className={`progress-container ${status === 'error' ? 'status-error' : ''} ${status === 'completed' ? 'status-completed' : ''}`}>
                    <div className="progress-label">
                        <span>
                            {progress < 100 ? 'Processing...' : 'Completed'} ({Math.round(progress)}%)
                        </span>
                        {progressStats && (
                            <span className="progress-stats">
                                {progressStats.processed.toLocaleString()} records processed
                                {progressStats.rate > 0 && ` (${Math.round(progressStats.rate).toLocaleString()}/sec)`}
                            </span>
                        )}
                    </div>
                    <div className="progress-bar">
                        <div 
                            className="progress-fill" 
                            style={{ width: `${progress}%` }}
                        ></div>
                    </div>
                </div>
            )}

            <div className="button-group">
                <button 
                    onClick={handlePreview} 
                    disabled={!isFormComplete || loading}
                    className="preview-button"
                >
                    {loading && !showPreview ? 'Loading Preview...' : 'Preview Data'}
                </button>
                <button 
                    onClick={handleStartIngest} 
                    disabled={!canStartIngest}
                    className="start-button"
                >
                    {loading ? 'Starting Ingestion...' : 'Start Ingestion'}
                </button>
            </div>
            <div className="ingestion-info">
                {direction === 'export' ? (
                    <p>This will export data from ClickHouse table "{tableName}" to file "{displayFilePath}".</p>
                ) : (
                    <p>This will import data from file "{displayFilePath}" to ClickHouse table "{tableName}".</p>
                )}
                <p>Selected columns: {selectedColumns.join(', ')}</p>
            </div>
            
            {/* Preview Modal */}
            {showPreview && previewDataRows && (
                <div className="modal">
                    <div className="modal-content preview-modal">
                        <span className="close" onClick={closePreview}>&times;</span>
                        <h3>Data Preview</h3>
                        <p>Showing first {previewDataRows.length} rows of data</p>
                        
                        <div className="preview-table-wrapper">
                            <table className="preview-table">
                                <thead>
                                    <tr>
                                        {selectedColumns.map(column => (
                                            <th key={column}>{column}</th>
                                        ))}
                                    </tr>
                                </thead>
                                <tbody>
                                    {previewDataRows.map((row, rowIndex) => (
                                        <tr key={rowIndex}>
                                            {row.map((cell, cellIndex) => (
                                                <td key={cellIndex}>{cell !== null ? String(cell) : 'NULL'}</td>
                                            ))}
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                        
                        <div className="preview-actions">
                            <button onClick={closePreview}>Close</button>
                            <button 
                                onClick={handleStartIngest} 
                                disabled={!canStartIngest}
                                className="start-button"
                            >
                                Proceed with Ingestion
                            </button>
                        </div>
                    </div>
                </div>
            )}

            {/* Debug info */}
            {process.env.NODE_ENV === 'development' && (
                <div className="debug-info" style={{fontSize: '10px', color: '#999', margin: '10px 0'}}>
                    <details>
                        <summary>Debug Info</summary>
                        <pre>
                            Operation ID: {operationId || 'none'}<br />
                            Status: {status}<br />
                            Progress: {progress}%<br />
                            {progressStats && `Records: ${progressStats.processed}/${progressStats.total || 'unknown'}`}
                        </pre>
                    </details>
                </div>
            )}
        </div>
    );
};

export default IngestControls;