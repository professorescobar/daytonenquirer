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
  let mount = form.querySelector('[data-turnstile]');
  if (!mount) {
    mount = document.createElement('div');
    mount.className = 'turnstile-widget';
    mount.dataset.turnstile = 'true';
    const submitBtn = form.querySelector('button[type="submit"]');
    if (submitBtn) {
      form.insertBefore(mount, submitBtn);
    } else {
      form.appendChild(mount);
    }
  }
  return mount;
}

async function setupTurnstile(forms) {
  const siteKey = await getTurnstileSiteKey();
  if (!siteKey) return;

  await loadTurnstileScript();
  if (!window.turnstile) return;

  forms.forEach((form) => {
    if (turnstileWidgetIds.has(form)) return;
    const mount = ensureTurnstileMount(form);
    const widgetId = window.turnstile.render(mount, {
      sitekey: siteKey,
      theme: 'auto'
    });
    turnstileWidgetIds.set(form, widgetId);
  });
}

async function submitNewsletterForm(form) {
  const emailInput = form.querySelector('input[name="email"]');
  const companyInput = form.querySelector('input[name="company"]');
  const submitBtn = form.querySelector('button[type="submit"]');
  const widgetId = turnstileWidgetIds.get(form);
  const email = String(emailInput?.value || '').trim();
  const turnstileToken = widgetId !== undefined && window.turnstile
    ? String(window.turnstile.getResponse(widgetId) || '').trim()
    : '';

  if (!email) {
    setSignupMessage(form, 'Enter your email address.', 'error');
    return;
  }

  if (widgetId !== undefined && !turnstileToken) {
    setSignupMessage(form, 'Please verify you are human.', 'error');
    return;
  }

  const originalBtnText = submitBtn ? submitBtn.textContent : '';
  if (submitBtn) {
    submitBtn.disabled = true;
    submitBtn.textContent = 'Signing up...';
  }

  try {
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
  } catch (error) {
    setSignupMessage(form, error.message || 'Signup failed. Please try again.', 'error');
  } finally {
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
  const forms = document.querySelectorAll('.newsletter-signup-form');
  setupTurnstile(forms).catch(() => {});
  forms.forEach((form) => {
    form.addEventListener('submit', (event) => {
      event.preventDefault();
      submitNewsletterForm(form);
    });
  });
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', bindNewsletterForms);
} else {
  bindNewsletterForms();
}
