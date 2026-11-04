(() => {
  const allowed = new Set(['login', 'page_view', 'ui_click', 'ui_change', 'form_submit', 'ticker_search', 'keyword_search', 'anychart_search', 'chart_interaction']);
  const recent = new Map();
  const clean = (value, limit = 120) => String(value || '').replace(/[\u0000-\u001f\u007f]/g, ' ').replace(/\s+/g, ' ').trim().slice(0, limit);
  const currentView = () => {
    for (const id of ['admin_view', 'anychart_view', 'gap_deepdive_view', 'multiday_view', 'tabs_view']) {
      const element = document.getElementById(id);
      if (element && getComputedStyle(element).display !== 'none') return id.replace(/_view$/, '') || 'terminal';
    }
    return location.pathname;
  };
  const context = () => ({ user: localStorage.getItem('gap_user') || '', token: localStorage.getItem('gap_token') || '' });
  const record = (action, label = '', details = {}) => {
    if (!allowed.has(action)) return;
    const auth = context();
    if (!auth.user || !auth.token) return;
    const page = `${location.pathname} · ${currentView()}`;
    const safeLabel = clean(label);
    const key = `${action}|${page}|${safeLabel}|${JSON.stringify(details)}`;
    const now = Date.now();
    if (now - (recent.get(key) || 0) < 900) return;
    recent.set(key, now);
    if (recent.size > 100) recent.delete(recent.keys().next().value);
    fetch('/api/activity', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, keepalive: true,
      body: JSON.stringify({ user: auth.user, token: auth.token, action, page, label: safeLabel, details })
    }).catch(() => {});
  };
  window.UserActivity = { record };

  const notice = document.createElement('div');
  notice.setAttribute('role', 'note');
  notice.textContent = 'Activity history is enabled for admin review: sign-ins, pages, tool actions, and submitted searches are logged. Passwords and unsent text are not recorded.';
  notice.style.cssText = 'padding:4px 18px;background:#080c10;border-bottom:1px solid #1c252d;color:#82919e;font:10px/1.4 Consolas,monospace;text-align:right';
  const mountNotice = () => { if (!notice.isConnected) document.body.prepend(notice); };
  if (document.body) mountNotice(); else document.addEventListener('DOMContentLoaded', mountNotice, { once: true });

  record('page_view', document.title || location.pathname);
  document.addEventListener('click', event => {
    const control = event.target.closest('button, a, [role="button"], [onclick], .ticker_tab, .user_card_row');
    if (!control || control.disabled || notice.contains(control)) return;
    const previousView = currentView();
    const label = control.getAttribute('aria-label') || control.title || control.innerText || control.textContent;
    const details = { control: control.tagName.toLowerCase() };
    if (control.matches('a[href]')) {
      try {
        const target = new URL(control.href, location.href);
        details.destination = target.origin === location.origin ? target.pathname : target.hostname;
      } catch {}
    }
    record('ui_click', label, details);
    setTimeout(() => { const nextView = currentView(); if (nextView !== previousView) record('page_view', nextView, { control: clean(label, 80) }); }, 0);
  }, true);
  document.addEventListener('change', event => {
    const control = event.target;
    if (!(control instanceof HTMLElement) || !control.matches('select, input[type="checkbox"], input[type="radio"]')) return;
    const label = control.labels?.[0]?.innerText || control.getAttribute('aria-label') || control.title || control.id || control.tagName;
    const details = { control: control.tagName.toLowerCase() };
    if (control instanceof HTMLInputElement) details.checked = control.checked;
    if (control instanceof HTMLSelectElement) details.selection = control.selectedOptions?.[0]?.textContent || '';
    record('ui_change', label, details);
  }, true);
  document.addEventListener('submit', event => {
    const form = event.target;
    if (!(form instanceof HTMLFormElement)) return;
    const label = form.getAttribute('aria-label') || form.id || form.querySelector('button[type="submit"],button:not([type])')?.innerText || 'Form';
    record('form_submit', label, { form: form.id || form.getAttribute('name') || 'form' });
  }, true);
  const chartDrags = new WeakMap();
  document.addEventListener('pointerdown', event => {
    if (event.target instanceof HTMLCanvasElement) chartDrags.set(event.target, { x: event.clientX, y: event.clientY, started: Date.now() });
  }, true);
  document.addEventListener('pointerup', event => {
    const canvas = event.target;
    if (!(canvas instanceof HTMLCanvasElement)) return;
    const start = chartDrags.get(canvas);chartDrags.delete(canvas);if (!start) return;
    const ticker = canvas._eventTicker || canvas.dataset.ticker || '';
    const moved = Math.hypot(event.clientX - start.x, event.clientY - start.y) > 7;
    const mode = canvas._avwapSelectMode ? 'AVWAP anchor' : canvas._rangeMeasureMode ? 'Measure range' : canvas._panToolMode ? 'Pan chart' : canvas._showVolumeProfile ? 'Volume profile range' : 'Chart zoom / selection';
    record('chart_interaction', moved ? mode : 'Chart click', { control: 'canvas', ticker });
  }, true);
  document.addEventListener('wheel', event => {
    if (!(event.target instanceof HTMLCanvasElement)) return;
    const ticker = event.target._eventTicker || event.target.dataset.ticker || '';
    record('chart_interaction', 'Chart wheel zoom', { control: 'canvas', ticker });
  }, { capture: true, passive: true });
  window.addEventListener('popstate', () => record('page_view', document.title || location.pathname));
})();
