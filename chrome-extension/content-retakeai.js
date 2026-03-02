/**
 * RetakeAI – Content script for RetakeAI website
 * Runs on: localhost:3000 and *.vercel.app
 *
 * Auto-fills the onboarding form with codes extracted from Steam.
 */

(function () {
  "use strict";

  function tryAutoFill() {
    const authInput = document.querySelector('input[name="auth_code"]');
    const tokenInput = document.querySelector('input[name="last_known_match_code"]');

    // Only fill if both fields exist (onboarding page) and are empty
    if (!authInput || !tokenInput) return;
    if (authInput.value && tokenInput.value) return;

    chrome.storage.local.get(
      ["retakeai_auth_code", "retakeai_match_token"],
      (data) => {
        if (chrome.runtime.lastError) return;

        const authCode = data.retakeai_auth_code;
        const matchToken = data.retakeai_match_token;

        if (!authCode && !matchToken) return;

        // Fill auth code
        if (authCode && !authInput.value) {
          setNativeValue(authInput, authCode);
        }

        // Fill match token
        if (matchToken && !tokenInput.value) {
          setNativeValue(tokenInput, matchToken);
        }

        showAutoFillBanner();
      }
    );
  }

  /**
   * Set value on a React-controlled input so React picks up the change.
   */
  function setNativeValue(element, value) {
    const nativeInputValueSetter = Object.getOwnPropertyDescriptor(
      window.HTMLInputElement.prototype,
      "value"
    ).set;
    nativeInputValueSetter.call(element, value);
    element.dispatchEvent(new Event("input", { bubbles: true }));
    element.dispatchEvent(new Event("change", { bubbles: true }));
  }

  function showAutoFillBanner() {
    if (document.getElementById("retakeai-autofill-banner")) return;

    const banner = document.createElement("div");
    banner.id = "retakeai-autofill-banner";
    banner.innerHTML = `
      <div style="
        position: fixed;
        top: 20px;
        right: 20px;
        z-index: 999999;
        display: flex;
        align-items: center;
        gap: 10px;
        background: linear-gradient(135deg, #1a1d21 0%, #0b0d0f 100%);
        border: 1px solid rgba(213, 255, 76, 0.3);
        border-radius: 14px;
        padding: 14px 20px;
        box-shadow: 0 20px 50px rgba(0,0,0,0.6), 0 0 30px rgba(213,255,76,0.08);
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
        animation: retakeai-slide-down 0.4s cubic-bezier(0.16, 1, 0.3, 1);
      ">
        <div style="
          width: 36px; height: 36px;
          border-radius: 10px;
          background: rgba(213,255,76,0.12);
          display: flex; align-items: center; justify-content: center;
        ">
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#d5ff4c" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
            <path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/>
          </svg>
        </div>
        <div>
          <div style="font-size: 13px; font-weight: 700; color: #fff; letter-spacing: 0.01em;">
            Auto-filled by RetakeAI
          </div>
          <div style="font-size: 11px; color: rgba(255,255,255,0.5); margin-top: 2px;">
            Codes from Steam were pre-filled. Just hit Save!
          </div>
        </div>
      </div>
    `;

    const style = document.createElement("style");
    style.textContent = `
      @keyframes retakeai-slide-down {
        from { opacity: 0; transform: translateY(-20px) scale(0.95); }
        to   { opacity: 1; transform: translateY(0) scale(1); }
      }
    `;
    document.head.appendChild(style);
    document.body.appendChild(banner);

    setTimeout(() => {
      banner.style.transition = "opacity 0.4s, transform 0.4s";
      banner.style.opacity = "0";
      banner.style.transform = "translateY(-10px)";
      setTimeout(() => banner.remove(), 400);
    }, 5000);
  }

  // Try immediately, and retry a few times for SPAs
  tryAutoFill();
  let attempts = 0;
  const interval = setInterval(() => {
    attempts++;
    tryAutoFill();
    if (attempts >= 10) clearInterval(interval);
  }, 1000);
})();
