import React, { useState, useEffect, useRef } from 'react';

export default function App() {
  const [query, setQuery] = useState('');
  const [category, setCategory] = useState('');
  const [inStockOnly, setInStockOnly] = useState(false);
  
  const [results, setResults] = useState([]);
  const [feed, setFeed] = useState([]);
  const [isLive, setIsLive] = useState(false);
  const [pulseActive, setPulseActive] = useState(false);
  
  const [config, setConfig] = useState({
    searchIndexUrl: 'http://localhost:7700',
    searchIndexApiKey: 'meili_master_key',
    apiBaseUrl: 'http://localhost:8000'
  });

  const categories = [
    'Electronics', 'Clothing', 'Books', 'Home & Kitchen', 
    'Beauty', 'Sports', 'Automotive', 'Toys', 'Office Products', 'Grocery'
  ];

  // 1. Fetch dynamic config from API backend
  useEffect(() => {
    fetch('/api/config')
      .then(res => res.json())
      .then(data => {
        if (data.searchIndexUrl) {
          setConfig(data);
        }
      })
      .catch(err => console.warn("Failed to load dynamic config, using default configuration.", err));
  }, []);

  // 2. Query Meilisearch whenever search inputs change
  useEffect(() => {
    const delayDebounceFn = setTimeout(() => {
      searchProducts();
    }, 200); // 200ms debounce

    return () => clearTimeout(delayDebounceFn);
  }, [query, category, inStockOnly, config]);

  const searchProducts = async () => {
    try {
      const filters = [];
      if (category) {
        filters.push(`category = "${category}"`);
      }
      if (inStockOnly) {
        filters.push(`in_stock = true`);
      }

      const response = await fetch(`${config.searchIndexUrl}/indexes/products/search`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${config.searchIndexApiKey}`
        },
        body: JSON.stringify({
          q: query,
          filter: filters.length > 0 ? filters.join(' AND ') : undefined,
          limit: 24
        })
      });

      if (response.ok) {
        const data = await response.json();
        setResults(data.hits || []);
      }
    } catch (err) {
      console.error("Meilisearch search error:", err);
    }
  };

  // 3. Connect to Server-Sent Events (SSE) Endpoint
  useEffect(() => {
    const sseUrl = `${config.apiBaseUrl}/api/cdc-stream`;
    logger("Connecting to SSE stream at " + sseUrl);
    
    const eventSource = new EventSource(sseUrl);
    
    eventSource.onopen = () => {
      setIsLive(true);
      logger("SSE connection established.");
    };

    eventSource.addEventListener('cdc_event', (event) => {
      try {
        const data = JSON.parse(event.data);
        
        // Trigger status indicator flash pulse
        setPulseActive(true);
        setTimeout(() => setPulseActive(false), 300);
        
        // Prepend event to live feed list
        setFeed(prevFeed => [data, ...prevFeed.slice(0, 49)]);
        
        // Refresh product list to show changes immediately
        searchProducts();
      } catch (err) {
        console.error("Failed to parse SSE payload:", err);
      }
    });

    eventSource.onerror = (err) => {
      setIsLive(false);
      console.error("SSE connection error:", err);
    };

    return () => {
      eventSource.close();
    };
  }, [config]);

  const logger = (msg) => {
    console.log(`[CDC-UI] ${msg}`);
  };

  const clearFeed = () => {
    setFeed([]);
  };

  return (
    <div className="bg-ambient-glow-wrapper">
      <div className="bg-ambient-glow"></div>
      <div className="bg-ambient-glow-secondary"></div>
      
      <div className="dashboard-container">
        {/* Header Section */}
        <header className="header">
          <div className="title-section">
            <h1>CDC Data Pipeline</h1>
            <p>Real-time logical replication from PostgreSQL to Meilisearch index</p>
          </div>
          
          <div className="status-pill">
            <div 
              data-testid="live-indicator" 
              className={`status-indicator ${isLive ? 'active' : ''} ${pulseActive ? 'pulse' : ''}`}
            ></div>
            <span className="status-text">
              {isLive ? 'Pipeline Live' : 'Connecting...'}
            </span>
          </div>
        </header>

        {/* Dashboard Content Grid */}
        <div className="dashboard-grid">
          
          {/* Catalog & Search Section */}
          <main className="glass-panel">
            <div className="search-controls">
              
              {/* Search Bar */}
              <div className="search-bar-wrapper">
                <input
                  type="text"
                  data-testid="search-input"
                  className="search-input"
                  placeholder="Search products by name, description, or category..."
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                />
              </div>

              {/* Filters */}
              <div className="filter-row">
                <div className="filter-select-wrapper">
                  <span className="filter-label">Category:</span>
                  <select 
                    className="filter-select"
                    value={category}
                    onChange={(e) => setCategory(e.target.value)}
                  >
                    <option value="">All Categories</option>
                    {categories.map(cat => (
                      <option key={cat} value={cat}>{cat}</option>
                    ))}
                  </select>
                </div>

                <div 
                  className={`toggle-switch-wrapper ${inStockOnly ? 'active' : ''}`}
                  onClick={() => setInStockOnly(!inStockOnly)}
                >
                  <div className="toggle-switch"></div>
                  <span className="filter-label">In Stock Only</span>
                </div>
              </div>
            </div>

            {/* Results Title & Count */}
            <div className="results-header">
              <span className="results-count">Showing {results.length} results</span>
            </div>

            {/* Results Grid */}
            <div data-testid="search-results" className="results-grid">
              {results.length > 0 ? (
                results.map(prod => (
                  <div key={prod.id} className="product-card">
                    <div>
                      <div className="product-header">
                        <span className="product-title" title={prod.name}>{prod.name}</span>
                        <span className="product-price">${prod.price.toFixed(2)}</span>
                      </div>
                      <p className="product-desc">{prod.description}</p>
                    </div>
                    <div className="product-footer">
                      <span className="product-category">{prod.category || 'Uncategorized'}</span>
                      <span className={`stock-badge ${prod.in_stock ? 'in-stock' : 'out-of-stock'}`}>
                        <span className="stock-dot"></span>
                        {prod.in_stock ? `In Stock (${prod.quantity})` : 'Out of Stock'}
                      </span>
                    </div>
                  </div>
                ))
              ) : (
                <div className="empty-state" style={{ gridColumn: '1 / -1' }}>
                  <h3>No products found</h3>
                  <p>Try refining your search query or filter settings.</p>
                </div>
              )}
            </div>
          </main>

          {/* Right Sidebar - Live CDC Feed */}
          <aside className="glass-panel sidebar-panel">
            <div className="panel-header">
              <h2>
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" style={{color: 'var(--accent-cyan)'}}>
                  <polyline points="22 12 18 12 15 21 9 3 6 12 2 12"></polyline>
                </svg>
                Live CDC Feed
              </h2>
              {feed.length > 0 && (
                <button className="feed-clear-btn" onClick={clearFeed}>Clear Log</button>
              )}
            </div>
            
            <div data-testid="cdc-feed" className="feed-container">
              {feed.length > 0 ? (
                feed.map((evt, idx) => (
                  <div key={idx} className="feed-item">
                    <div className="feed-item-header">
                      <span className="feed-table-badge">table: {evt.table}</span>
                      <span className={`feed-op-badge ${evt.operation.toLowerCase()}`}>
                        {evt.operation}
                      </span>
                    </div>
                    <span className="feed-item-time">
                      {new Date(evt.timestamp).toLocaleTimeString()}
                    </span>
                  </div>
                ))
              ) : (
                <div className="empty-state">
                  <p>Waiting for database changes...</p>
                  <p style={{fontSize: '0.8rem', marginTop: '0.5rem'}}>Perform INSERT, UPDATE, or DELETE on PostgreSQL products or inventory tables to stream events here.</p>
                </div>
              )}
            </div>
          </aside>

        </div>
      </div>
    </div>
  );
}
