import { useState } from 'react';
import './App.css';
import './credit.css';
import { DashboardPage, CreditTradingPage } from './pages';

/**
 * App - Main application shell with simple page navigation
 * 
 * Manages navigation between:
 * - Order Dashboard (original DBSP demo)
 * - Credit Trading (systematic credit trading simulation)
 */
function App() {
  const [currentPage, setCurrentPage] = useState<'dashboard' | 'credit'>('dashboard');

  return (
    <div className="app-container">
      {/* Navigation Bar */}
      <nav className="app-nav">
        <div className="nav-brand">
          <span className="nav-logo">◆</span>
          <span className="nav-title">DBSP</span>
        </div>
        <div className="nav-links">
          <button
            className={`nav-link ${currentPage === 'dashboard' ? 'active' : ''}`}
            onClick={() => setCurrentPage('dashboard')}
          >
            📦 Orders
          </button>
          <button
            className={`nav-link ${currentPage === 'credit' ? 'active' : ''}`}
            onClick={() => setCurrentPage('credit')}
          >
            💹 Credit Trading
          </button>
        </div>
        <div className="nav-info">
          <span>Incremental View Maintenance</span>
        </div>
      </nav>

      {/* Page Content */}
      <div className="page-content">
        {currentPage === 'dashboard' && <DashboardPage />}
        {currentPage === 'credit' && <CreditTradingPage />}
      </div>
    </div>
  );
}

export default App;
