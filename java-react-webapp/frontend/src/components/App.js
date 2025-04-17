import React from 'react';
import { BrowserRouter as Router, Route, Switch, Link } from 'react-router-dom';
import Home from './Home';
import ClickHouseForm from './ClickHouseForm';
import FileToClickHouse from './FileToClickHouse'; 
import '../styles/App.css';

function App() {
    return (
        <Router>
            <div className="app-container">
                <header className="app-header">
                    <h1>ClickHouse-Flat File Connector</h1>
                    <nav>
                        <ul>
                            <li><Link to="/">Home</Link></li>
                            <li><Link to="/clickhouse-to-file">ClickHouse to File</Link></li>
                            <li><Link to="/file-to-clickhouse">File to ClickHouse</Link></li>
                        </ul>
                    </nav>
                </header>
                
                <main className="app-content">
                    <Switch>
                        <Route path="/" exact component={Home} />
                        <Route path="/clickhouse-to-file" render={() => <ClickHouseForm direction="export" />} />
                        <Route path="/file-to-clickhouse" component={FileToClickHouse} />
                    </Switch>
                </main>
                
                <footer className="app-footer">
                    <p>© 2025 ClickHouse-Flat File Connector</p>
                </footer>
            </div>
        </Router>
    );
}

export default App;