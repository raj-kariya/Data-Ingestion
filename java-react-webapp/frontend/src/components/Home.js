import React from 'react';
import { Link } from 'react-router-dom';

const Home = () => {
    return (
        <div className="home-container">
            <h1>Welcome to ClickHouse-Flat File Connector</h1>
            <p>
                This application allows you to transfer data between ClickHouse databases 
                and flat files (CSV). Choose one of the options below to get started:
            </p>
            
            <div className="options-grid">
                <div className="option-card">
                    <h2>ClickHouse to File</h2>
                    <p>Export data from a ClickHouse table to a CSV file</p>
                    <Link to="/clickhouse-to-file" className="option-button">
                        Export Data
                    </Link>
                </div>
                
                <div className="option-card">
                    <h2>File to ClickHouse</h2>
                    <p>Import data from a CSV file to a ClickHouse table</p>
                    <Link to="/file-to-clickhouse" className="option-button">
                        Import Data
                    </Link>
                </div>
            </div>
            
            <div className="features-section">
                <h2>Features</h2>
                <ul>
                    <li>Connect to any ClickHouse instance</li>
                    <li>Select specific columns for import/export</li>
                    <li>Support for various CSV delimiters</li>
                    <li>Automatic data type inference</li>
                    <li>High-performance data transfer</li>
                </ul>
            </div>
        </div>
    );
};

export default Home;