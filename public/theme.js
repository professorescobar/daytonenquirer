(() => {
  const STORAGE_KEY = 'de_theme';

  const TICKER_BASE_CONFIG = {
    symbols: [
      { proName: 'DJI', title: 'Dow Jones' },
      { proName: 'OANDA:SPX500USD', title: 'S&P 500' },
      { proName: 'OANDA:NAS100USD', title: 'NASDAQ 100' },
      { proName: 'NYSE:NYA', title: 'NYSE Composite' },
      { proName: 'OANDA:US2000USD', title: 'Russell 2000' },
      { proName: 'OANDA:EURUSD', title: 'EUR/USD' },
      { proName: 'OANDA:USDJPY', title: 'USD/JPY' },
      { proName: 'TVC:GOLD', title: 'Gold' },
      { proName: 'TVC:SILVER', title: 'Silver' },
      { proName: 'TVC:USOIL', title: 'Crude Oil' }
    ],
    showSymbolLogo: false,
    showChange: true,
    showPercentageChange: true,
    colorTheme: 'light',
    isTransparent: false,
    displayMode: 'regular',
    locale: 'en'
  };

  function getSystemTheme() {
    return window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
  }

  function getStoredTheme() {
    const stored = localStorage.getItem(STORAGE_KEY);
    return stored === 'dark' || stored === 'light' ? stored : null;
  }

  function getTheme() {
    return getStoredTheme() || getSystemTheme();
  }

  function applyTheme(theme, persist) {
    const resolved = theme === 'dark' ? 'dark' : 'light';
    document.documentElement.setAttribute('data-theme', resolved);
    document.documentElement.style.colorScheme = resolved;

    if (persist) {
      localStorage.setItem(STORAGE_KEY, resolved);
    }

    updateToggleState();
    window.dispatchEvent(new CustomEvent('de-theme-changed', { detail: { theme: resolved } }));
  }

  function updateToggleState() {
    const theme = document.documentElement.dataset.theme === 'dark' ? 'dark' : 'light';
    const toggles = document.querySelectorAll('[data-theme-toggle]');

    toggles.forEach((toggle) => {
      const next = theme === 'dark' ? 'light' : 'dark';
      toggle.setAttribute('aria-pressed', theme === 'dark' ? 'true' : 'false');
      toggle.setAttribute('aria-label', 'Switch to ' + next + ' mode');
      toggle.setAttribute('title', 'Switch to ' + next + ' mode');
    });
  }

  function bindToggles() {
    const toggles = document.querySelectorAll('[data-theme-toggle]');
    toggles.forEach((toggle) => {
      if (toggle.dataset.themeBound === 'true') return;
      toggle.dataset.themeBound = 'true';

      toggle.addEventListener('click', () => {
        const current = document.documentElement.dataset.theme === 'dark' ? 'dark' : 'light';
        const next = current === 'dark' ? 'light' : 'dark';
        applyTheme(next, true);
      });
    });

    updateToggleState();
  }

  function renderTicker(container, theme) {
    container.innerHTML = '';

    const widgetRoot = document.createElement('div');
    widgetRoot.className = 'tradingview-widget-container__widget';
    container.appendChild(widgetRoot);

    const script = document.createElement('script');
    script.src = 'https://s3.tradingview.com/external-embedding/embed-widget-ticker-tape.js';
    script.async = true;
    script.innerHTML = JSON.stringify({ ...TICKER_BASE_CONFIG, colorTheme: theme });
    container.appendChild(script);

    container.dataset.tvTheme = theme;
  }

  function syncTradingViewTickerTheme(force) {
    const targetTheme = document.documentElement.dataset.theme === 'dark' ? 'dark' : 'light';
    const containers = document.querySelectorAll('.tradingview-widget-container');

    containers.forEach((container) => {
      if (!force && container.dataset.tvTheme === targetTheme) return;
      renderTicker(container, targetTheme);
    });
  }

  applyTheme(getTheme(), false);

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', bindToggles);
  } else {
    bindToggles();
  }

  window.addEventListener('load', () => {
    setTimeout(() => syncTradingViewTickerTheme(true), 80);
  });

  window.addEventListener('de-theme-changed', () => {
    syncTradingViewTickerTheme(true);
  });

  const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
  if (mediaQuery && mediaQuery.addEventListener) {
    mediaQuery.addEventListener('change', (event) => {
      if (!getStoredTheme()) {
        applyTheme(event.matches ? 'dark' : 'light', false);
      }
    });
  }
})();
