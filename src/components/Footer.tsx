export function Footer() {
  return (
    <footer className="stress-footer">
      <div className="theory-cards">
        <div className="theory-card">
          <h3>Bilinear JOIN</h3>
          <p>Incremental join updates when either side changes.</p>
          <code>{`Δ(a ⋈ b) = Δa ⋈ Δb + a ⋈ Δb + Δa ⋈ b`}</code>
        </div>
        <div className="theory-card highlight">
          <h3>🚀 Append-Only Mode</h3>
          <p>3000x+ faster for insert-only streams!</p>
          <code>{`mode: 'append-only' // Skip deletion tracking`}</code>
        </div>
        <div className="theory-card">
          <h3>Semi & Anti Joins</h3>
          <p>Existence checks without full join cost.</p>
          <code>{`WHERE EXISTS / WHERE NOT EXISTS`}</code>
        </div>
      </div>
      <p className="footer-links">
        <a href="https://www.vldb.org/pvldb/vol16/p1601-budiu.pdf" target="_blank" rel="noopener">DBSP Paper</a>
        {' • '}
        <a href="https://github.com/feldera/feldera" target="_blank" rel="noopener">Feldera</a>
        {' • '}
        <span className="sql-count">8 SQL Views + Optimized JOINs</span>
      </p>
    </footer>
  );
}

