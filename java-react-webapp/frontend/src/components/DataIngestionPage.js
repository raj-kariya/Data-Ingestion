import React, { useState } from 'react';
import FileUploader from './FileUploader';
import ClickHouseForm from './ClickHouseForm';
import IngestControls from './IngestControls';
const handleStartIngest= () => {
    // Handle the start of the ingestion process
    console.log('Ingestion started!');
};
const DataIngestionPage = () => {
    const [filePath, setFilePath] = useState('');
    const [connection, setConnection] = useState(null);
    const [tableName, setTableName] = useState('');
    const [selectedColumns, setSelectedColumns] = useState([]);
    const [showClickHouseForm, setShowClickHouseForm] = useState(false);
    
    const handleFileUploaded = (uploadedFilePath, columns) => {
        setFilePath(uploadedFilePath);
        // Store columns if needed
    };
    
    const handleConnectionConfig = (config) => {
        setConnection(config.connection);
        setTableName(config.tableName);
        setSelectedColumns(config.selectedColumns);
        setShowClickHouseForm(false);
    };
    
    return (
        <div className="data-ingestion-page">
            <FileUploader 
                direction="import"
                onFileUploaded={handleFileUploaded}
                connection={connection}
                tableName={tableName}
                selectedColumns={selectedColumns}
                showClickHouseForm={showClickHouseForm}
                setShowClickHouseForm={setShowClickHouseForm}
            />
            
            {showClickHouseForm && (
                <div className="modal">
                    <div className="modal-content">
                        <span className="close" onClick={() => setShowClickHouseForm(false)}>&times;</span>
                        <ClickHouseForm 
                            direction="import" 
                            onConnectionConfig={handleConnectionConfig}
                            filePath={filePath}
                        />
                    </div>
                </div>
            )}
            
            {/* Render IngestControls here instead of in child components */}
            {filePath && connection && tableName && selectedColumns.length > 0 && (
                <IngestControls 
                    direction="import"
                    connection={connection}
                    tableName={tableName}
                    selectedColumns={selectedColumns}
                    filePath={filePath}
                    onStartIngest={handleStartIngest}
                />
            )}
        </div>
    );
};

export default DataIngestionPage;