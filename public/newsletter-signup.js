function setSignupMessage(form, text, kind) {
  const message = form?.querySelector('[data-newsletter-message]');
  if (!message) return;
  message.textContent = text || '';
  message.classList.remove('is-success', 'is-error');
  if (kind === 'success') message.classList.add('is-success');
  if (kind === 'error') message.classList.add('is-error');
}

let turnstileSiteKeyPromise = null;
let turnstileScriptPromise = null;
const turnstileWidgetIds = new WeakMap();
const turnstilePending = new WeakMap();
const MOBILE_BREAKPOINT = 768;

function isMobileSignupLayout() {
  return window.matchMedia(`(max-width: ${MOBILE_BREAKPOINT}px)`).matches;
}

function getSignupStrip(form) {
  return form?.closest('.newsletter-strip') || null;
}

function getMobileToggle(form) {
  const strip = getSignupStrip(form);
  return strip?.querySelector('[data-newsletter-toggle]') || null;
}

function setMobileExpanded(form, isExpanded, options = {}) {
  const strip = getSignupStrip(form);
  const toggle = getMobileToggle(form);
  if (!strip || !toggle || !isMobileSignupLayout()) return;

  strip.classList.toggle('is-mobile-expanded', isExpanded);
  strip.classList.toggle('is-mobile-collapsed', !isExpanded);
  toggle.setAttribute('aria-expanded', isExpanded ? 'true' : 'false');
  toggle.classList.toggle('is-close', isExpanded);
  toggle.textContent = isExpanded ? 'Close' : 'Sign Up';
  toggle.setAttribute('aria-label', isExpanded ? 'Close newsletter signup' : 'Open newsletter signup');

  if (isExpanded && options.focusInput) {
    const emailInput = form.querySelector('input[name="email"]');
    setTimeout(() => emailInput?.focus(), 0);
  }

}

function syncMobileSignupState(forms) {
  forms.forEach((form) => {
    const strip = getSignupStrip(form);
    const toggle = getMobileToggle(form);
    if (!strip || !toggle) return;

    if (isMobileSignupLayout()) {
      if (strip.classList.contains('is-mobile-expanded')) {
        toggle.setAttribute('aria-expanded', 'true');
        toggle.classList.add('is-close');
        toggle.textContent = 'Close';
        toggle.setAttribute('aria-label', 'Close newsletter signup');
      } else {
        strip.classList.add('is-mobile-collapsed');
        strip.classList.remove('is-mobile-expanded');
        toggle.setAttribute('aria-expanded', 'false');
        toggle.classList.remove('is-close'); // This is fine, it should not be 'is-close' when collapsed
        toggle.textContent = 'Sign Up';
        toggle.setAttribute('aria-label', 'Open newsletter signup');
      }
      return;
    }

    strip.classList.remove('is-mobile-collapsed', 'is-mobile-expanded');
    toggle.setAttribute('aria-expanded', 'false');
    toggle.classList.remove('is-close');
    toggle.textContent = 'Sign Up';
    toggle.setAttribute('aria-label', 'Open newsletter signup');
  });
}

function bindArticleInlineCtas() {
  const ctas = Array.from(document.querySelectorAll('[data-newsletter-cta]'));
  ctas.forEach((cta) => {
    const toggle = cta.querySelector('[data-newsletter-inline-toggle]');
    const form = cta.querySelector('.article-newsletter-form.newsletter-signup-form');
    if (!toggle || !form) return;

    toggle.addEventListener('click', () => {
      cta.classList.add('is-inline-expanded');
      toggle.setAttribute('aria-expanded', 'true');
      const emailInput = form.querySelector('input[name="email"]');
      setTimeout(() => emailInput?.focus(), 0);
      ensureTurnstileWidget(form).catch(() => null);
    });
  });
}

function syncArticleInlineCtaLabels() {
  const isMobile = window.matchMedia(`(max-width: ${MOBILE_BREAKPOINT}px)`).matches;
  const labels = Array.from(document.querySelectorAll('[data-newsletter-cta] [data-newsletter-inline-toggle]'));
  labels.forEach((button) => {
    button.textContent = isMobile ? 'JOIN OUR NEWSLETTER' : 'Get Weekly Updates';
  });
}

function collapseArticleInlineCta(cta) {
  if (!cta) return;
  cta.classList.remove('is-inline-expanded');
  const toggle = cta.querySelector('[data-newsletter-inline-toggle]');
  if (toggle) toggle.setAttribute('aria-expanded', 'false');
  const form = cta.querySelector('.article-newsletter-form.newsletter-signup-form');
  if (form) setSignupMessage(form, '', '');
}

async function getTurnstileSiteKey() {
  if (!turnstileSiteKeyPromise) {
    turnstileSiteKeyPromise = fetch('/api/newsletter-signup-config')
      .then((res) => (res.ok ? res.json() : {}))
      .then((data) => String(data?.turnstileSiteKey || '').trim())
      .catch(() => '');
  }
  return turnstileSiteKeyPromise;
}

async function loadTurnstileScript() {
  if (window.turnstile) return;
  if (!turnstileScriptPromise) {
    turnstileScriptPromise = new Promise((resolve, reject) => {
      const existing = document.querySelector('script[data-turnstile-script]');
      if (existing) {
        existing.addEventListener('load', () => resolve(), { once: true });
        existing.addEventListener('error', () => reject(new Error('Turnstile failed to load')), { once: true });
        return;
      }

      const script = document.createElement('script');
      script.src = 'https://challenges.cloudflare.com/turnstile/v0/api.js?render=explicit';
      script.async = true;
      script.defer = true;
      script.dataset.turnstileScript = 'true';
      script.onload = () => resolve();
      script.onerror = () => reject(new Error('Turnstile failed to load'));
      document.head.appendChild(script);
    });
  }
  return turnstileScriptPromise;
}

function ensureTurnstileMount(form) {
  let mount = form.querySelector('[data-turnstile-hidden]');
  if (!mount) {
    mount = document.createElement('div');
    mount.dataset.turnstileHidden = 'true';
    mount.hidden = true;
    form.appendChild(mount);
  }
  return mount;
}

async function ensureTurnstileWidget(form) {
  const siteKey = await getTurnstileSiteKey();
  if (!siteKey) return null;

  await loadTurnstileScript();
  if (!window.turnstile) return null;

  const existingId = turnstileWidgetIds.get(form);
  if (existingId !== undefined) return existingId;

  const mount = ensureTurnstileMount(form);
  const widgetId = window.turnstile.render(mount, {
    sitekey: siteKey,
    size: 'invisible',
    theme: 'auto',
    callback: (token) => {
      const pending = turnstilePending.get(form);
      if (pending) {
        clearTimeout(pending.timeoutId);
        turnstilePending.delete(form);
        pending.resolve(String(token || '').trim());
      }
    },
    'error-callback': () => {
      const pending = turnstilePending.get(form);
      if (pending) {
        clearTimeout(pending.timeoutId);
        turnstilePending.delete(form);
        pending.reject(new Error('Verification failed. Please try again.'));
      }
    },
    'expired-callback': () => {
      const pending = turnstilePending.get(form);
      if (pending) {
        clearTimeout(pending.timeoutId);
        turnstilePending.delete(form);
        pending.reject(new Error('Verification expired. Please try again.'));
      }
    }
  });
  turnstileWidgetIds.set(form, widgetId);
  return widgetId;
}

async function getTurnstileToken(form) {
  const widgetId = await ensureTurnstileWidget(form);
  if (widgetId === null || widgetId === undefined || !window.turnstile) return '';

  window.turnstile.reset(widgetId);

  return new Promise((resolve, reject) => {
    const timeoutId = setTimeout(() => {
      if (turnstilePending.has(form)) {
        turnstilePending.delete(form);
        reject(new Error('Verification timed out. Please try again.'));
      }
    }, 12000);

    turnstilePending.set(form, { resolve, reject, timeoutId });

    try {
      window.turnstile.execute(widgetId);
    } catch (_) {
      clearTimeout(timeoutId);
      turnstilePending.delete(form);
      reject(new Error('Verification failed. Please try again.'));
    }
  });
}

async function submitNewsletterForm(form) {
  const emailInput = form.querySelector('input[name="email"]');
  const companyInput = form.querySelector('input[name="company"]');
  const submitBtn = form.querySelector('button[type="submit"]');
  const email = String(emailInput?.value || '').trim();
  const siteKey = await getTurnstileSiteKey();
  const requiresTurnstile = !!siteKey;

  if (!email) {
    setSignupMessage(form, 'Enter your email address.', 'error');
    return;
  }

  const originalBtnText = submitBtn ? submitBtn.textContent : '';
  if (submitBtn) {
    submitBtn.disabled = true;
    submitBtn.textContent = 'Signing up...';
  }

  try {
    const turnstileToken = requiresTurnstile ? await getTurnstileToken(form) : '';

    if (requiresTurnstile && !turnstileToken) {
      throw new Error('Please verify you are human.');
    }

    const res = await fetch('/api/newsletter-subscribe', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        email,
        source: form.dataset.source || '',
        section: form.dataset.section || '',
        company: companyInput ? companyInput.value : '',
        turnstileToken
      })
    });

    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.error || 'Signup failed');
    setSignupMessage(form, data.alreadyExists ? 'You are already subscribed.' : 'You are subscribed. Check your inbox.', 'success');
    form.reset();
    if (isMobileSignupLayout()) {
      setMobileExpanded(form, false);
    }
  } catch (error) {
    setSignupMessage(form, error.message || 'Signup failed. Please try again.', 'error');
  } finally {
    const widgetId = turnstileWidgetIds.get(form);
    if (widgetId !== undefined && window.turnstile) {
      window.turnstile.reset(widgetId);
    }
    if (submitBtn) {
      submitBtn.disabled = false;
      submitBtn.textContent = originalBtnText || 'Sign Up!';
    }
  }
}

function bindNewsletterForms() {
  const forms = Array.from(document.querySelectorAll('.newsletter-signup-form'));
  const articleInlineCtas = Array.from(document.querySelectorAll('[data-newsletter-cta]'));
  getTurnstileSiteKey().catch(() => '');
  bindArticleInlineCtas();
  syncArticleInlineCtaLabels();
  syncMobileSignupState(forms);

  forms.forEach((form) => {
    const toggle = getMobileToggle(form);
    if (toggle) {
      toggle.addEventListener('click', () => {
        if (!isMobileSignupLayout()) return;
        const strip = getSignupStrip(form);
        const isExpanded = strip?.classList.contains('is-mobile-expanded');
        setMobileExpanded(form, !isExpanded, { focusInput: !isExpanded });
      });
    }

    form.addEventListener('submit', (event) => {
      event.preventDefault();
      submitNewsletterForm(form);
    });
  });

  document.addEventListener('pointerdown', (event) => {
    forms.forEach((form) => {
      const strip = getSignupStrip(form);
      if (!strip || !isMobileSignupLayout()) return;
      if (!strip.contains(event.target) && strip.classList.contains('is-mobile-expanded')) {
        setMobileExpanded(form, false);
      }
    });

    articleInlineCtas.forEach((cta) => {
      if (!cta.classList.contains('is-inline-expanded')) return;
      if (cta.contains(event.target)) return;
      collapseArticleInlineCta(cta);
    });
  });

  window.addEventListener('resize', () => {
    syncArticleInlineCtaLabels();
    syncMobileSignupState(forms);
  });
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', bindNewsletterForms);
} else {
  bindNewsletterForms();
}
