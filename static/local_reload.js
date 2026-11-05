(() => {
  let version = null;
  let inFlight = false;
  async function poll() {
    if (inFlight || document.hidden) return;
    inFlight = true;
    try {
      const response = await fetch(`/api/local-reload-version?t=${Date.now()}`, {cache: 'no-store'});
      if (!response.ok) return;
      const state = await response.json();
      if (version === null) version = state.version;
      else if (state.version !== version) location.reload();
    } catch (_) {
      // The local server may be restarting; keep polling until it returns.
    } finally {
      inFlight = false;
    }
  }
  setInterval(poll, 1200);
  poll();
})();
