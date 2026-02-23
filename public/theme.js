(() => {
  const STORAGE_KEY = 'de_theme';

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

  function syncTradingViewTickerTheme() {
    const targetTheme = document.documentElement.dataset.theme === 'dark' ? 'dark' : 'light';
    const containers = document.querySelectorAll('.tradingview-widget-container');

    containers.forEach((container) => {
      const existingScript = container.querySelector('script[src*="embed-widget-ticker-tape.js"]');
      if (!existingScript) return;

      let config;
      try {
        config = JSON.parse(existingScript.textContent || existingScript.innerHTML || '{}');
      } catch (_) {
        return;
      }

      if (!config || config.colorTheme === targetTheme) return;

      config.colorTheme = targetTheme;

      container.innerHTML = '';
      const widgetRoot = document.createElement('div');
      widgetRoot.className = 'tradingview-widget-container__widget';
      container.appendChild(widgetRoot);

      const script = document.createElement('script');
      script.src = 'https://s3.tradingview.com/external-embedding/embed-widget-ticker-tape.js';
      script.async = true;
      script.innerHTML = JSON.stringify(config);
      container.appendChild(script);
    });
  }

  applyTheme(getTheme(), false);

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', bindToggles);
  } else {
    bindToggles();
  }

  window.addEventListener('load', () => {
    setTimeout(syncTradingViewTickerTheme, 80);
  });

  window.addEventListener('de-theme-changed', () => {
    setTimeout(syncTradingViewTickerTheme, 10);
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
