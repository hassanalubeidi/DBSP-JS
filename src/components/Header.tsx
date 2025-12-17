interface HeaderProps {
  connectionState: 'disconnected' | 'connecting' | 'connected';
}

export function Header({ connectionState }: HeaderProps) {
  return (
    <header className="stress-header">
      <div className="header-content">
        <h1 className="stress-title">
          <span className="title-icon">●</span>
          DBSP Real-Time Monitor
        </h1>
        <p className="stress-subtitle">
          100K Orders ⋈ 10K Customers • Delta Stream • 8 SQL Views with JOIN Demo
        </p>
      </div>
      <div className={`connection-badge ${connectionState}`}>
        <span className="connection-dot"></span>
        {connectionState === 'connected' ? 'LIVE' : 
         connectionState === 'connecting' ? 'CONNECTING' : 'OFFLINE'}
      </div>
    </header>
  );
}

