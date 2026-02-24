function setSignupMessage(form, text, kind) {
  const message = form?.querySelector('[data-newsletter-message]');
  if (!message) return;
  message.textContent = text || '';
  message.classList.remove('is-success', 'is-error');
  if (kind === 'success') message.classList.add('is-success');
  if (kind === 'error') message.classList.add('is-error');
}

async function submitNewsletterForm(form) {
  const emailInput = form.querySelector('input[name="email"]');
  const companyInput = form.querySelector('input[name="company"]');
  const submitBtn = form.querySelector('button[type="submit"]');
  const email = String(emailInput?.value || '').trim();

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
    const res = await fetch('/api/newsletter-subscribe', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        email,
        source: form.dataset.source || '',
        section: form.dataset.section || '',
        company: companyInput ? companyInput.value : ''
      })
    });

    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.error || 'Signup failed');
    setSignupMessage(form, data.alreadyExists ? 'You are already subscribed.' : 'You are subscribed. Check your inbox.', 'success');
    form.reset();
  } catch (error) {
    setSignupMessage(form, error.message || 'Signup failed. Please try again.', 'error');
  } finally {
    if (submitBtn) {
      submitBtn.disabled = false;
      submitBtn.textContent = originalBtnText || 'Sign Up!';
    }
  }
}

function bindNewsletterForms() {
  const forms = document.querySelectorAll('.newsletter-signup-form');
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
