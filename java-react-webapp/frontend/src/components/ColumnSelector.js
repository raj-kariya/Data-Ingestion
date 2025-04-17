import React from 'react';

const ColumnSelector = ({ columns, selectedColumns, onColumnSelect }) => {
    const handleSelectAll = () => {
        columns.forEach(column => {
            if (!selectedColumns.includes(column.name)) {
                onColumnSelect(column.name, true);
            }
        });
    };
    
    const handleUnselectAll = () => {
        selectedColumns.forEach(columnName => {
            onColumnSelect(columnName, false);
        });
    };
    
    const handleCheckboxChange = (e, columnName) => {
        onColumnSelect(columnName, e.target.checked);
    };
    
    return (
        <div className="column-selector">
            <h3>Column Selection</h3>
            <div className="column-controls">
                <button onClick={handleSelectAll}>Select All</button>
                <button onClick={handleUnselectAll}>Unselect All</button>
                <span>{selectedColumns.length} of {columns.length} columns selected</span>
            </div>
            <div className="columns-list">
                {columns.map(column => (
                    <div key={column.name} className="column-item">
                        <label>
                            <input 
                                type="checkbox" 
                                checked={selectedColumns.includes(column.name)} 
                                onChange={(e) => handleCheckboxChange(e, column.name)} 
                            />
                            <span className="column-name">{column.name}</span>
                            <span className="column-type">{column.type}</span>
                        </label>
                    </div>
                ))}
            </div>
        </div>
    );
};

export default ColumnSelector;