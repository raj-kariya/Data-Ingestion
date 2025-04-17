import React, { useState } from 'react';
import { testConnection } from '../services/api';

const ClickHouseConnectionForm = ({ onConnectionSuccess }) => {
    const [connection, setConnection] = useState({
        host: 'localhost',
        port: 8123,
        database: 'default',
        user: 'default',
        jwtToken: ''
    });
    const [status, setStatus] = useState('idle');
    const [error, setError] = useState(null);
    
    const handleConnectionChange = (e) => {
        const { name, value } = e.target;
        setConnection(prev => ({ ...prev, [name]: name === 'port' ? parseInt(value) : value }));
    };
    
    const handleConnect = async () => {
        setStatus('connecting');
        setError(null);
        
        try {
            const success = await testConnection(connection);
            if (success) {
                setStatus('connected');
                // Pass connection details back to parent
                onConnectionSuccess(connection);
            } else {
                setStatus('error');
                setError('Failed to connect to ClickHouse');
            }
        } catch (error) {
            setStatus('error');
            setError(`Error: ${error.message}`);
        }
    };
    
    return (
        <div className="connection-form">
            <h3>ClickHouse Connection</h3>
            <div className="form-group">
                <label>Host:</label>
                <input 
                    type="text" 
                    name="host" 
                    value={connection.host} 
                    onChange={handleConnectionChange} 
                />
            </div>
            <div className="form-group">
                <label>Port:</label>
                <input 
                    type="number" 
                    name="port" 
                    value={connection.port} 
                    onChange={handleConnectionChange} 
                />
            </div>
            <div className="form-group">
                <label>Database:</label>
                <input 
                    type="text" 
                    name="database" 
                    value={connection.database} 
                    onChange={handleConnectionChange} 
                />
            </div>
            <div className="form-group">
                <label>User:</label>
                <input 
                    type="text" 
                    name="user" 
                    value={connection.user} 
                    onChange={handleConnectionChange} 
                />
            </div>
            <div className="form-group">
                <label>JWT Token (optional):</label>
                <input 
                    type="password" 
                    name="jwtToken" 
                    value={connection.jwtToken} 
                    onChange={handleConnectionChange} 
                />
            </div>
            
            <button 
                onClick={handleConnect} 
                disabled={status === 'connecting'}
                className="connect-button"
            >
                {status === 'connecting' ? 'Connecting...' : 'Connect'}
            </button>
            
            {status === 'error' && error && (
                <div className="error-message">
                    {error}
                </div>
            )}
            
            {status === 'connected' && (
                <div className="success-message">
                    Connected successfully!
                </div>
            )}
        </div>
    );
};

export default ClickHouseConnectionForm;