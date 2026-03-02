/**
 * RetakeAI – Content script for Steam GCPD page
 * Runs on: https://help.steampowered.com/*
 *
 * Extracts the Game Authentication Code and Last Match Sharing Code
 * from the Steam Personal Game Data page for CS2.
 */

(function () {
  "use strict";

  const AUTH_CODE_RE = /\b([A-Z0-9]{4}-[A-Z0-9]{5}-[A-Z0-9]{4})\b/;
  const MATCH_TOKEN_RE = /\b(CSGO-[A-Za-z0-9]{5}(?:-[A-Za-z0-9]{5}){4})\b/;

  /** Scan visible text + input values on the page */
  function scanPage() {
    const body = document.body?.innerText || "";
    const inputs = Array.from(document.querySelectorAll("input, textarea, code, pre, span, td, div"));

    let authCode = null;
    let matchToken = null;

    // 1. Try input/textarea values first (Steam sometimes uses inputs)
    for (const el of inputs) {
      const val = (el.value || el.textContent || "").trim();
      if (!authCode) {
        const m = val.match(AUTH_CODE_RE);
        if (m) authCode = m[1].toUpperCase();
      }
      if (!matchToken) {
        const m = val.match(MATCH_TOKEN_RE);
        if (m) matchToken = m[1];
      }
      if (authCode && matchToken) break;
    }

    // 2. Fallback: scan full body text
    if (!authCode) {
      const m = body.match(AUTH_CODE_RE);
      if (m) authCode = m[1].toUpperCase();
    }
    if (!matchToken) {
      const m = body.match(MATCH_TOKEN_RE);
      if (m) matchToken = m[1];
    }

    return { authCode, matchToken };
  }

  /** Store codes and notify extension */
  function storeAndNotify(codes) {
    const payload = {};
    if (codes.authCode) payload.retakeai_auth_code = codes.authCode;
    if (codes.matchToken) payload.retakeai_match_token = codes.matchToken;

    if (Object.keys(payload).length === 0) return;

    // Add timestamp
    payload.retakeai_extracted_at = new Date().toISOString();

    chrome.storage.local.set(payload, () => {
      // Notify background / popup that new data is available
      chrome.runtime.sendMessage({
        type: "CODES_EXTRACTED",
        authCode: codes.authCode || null,
        matchToken: codes.matchToken || null,
      });
    });

    // Show a subtle toast on the Steam page
    showToast(codes);
  }

  function showToast(codes) {
    // Don't show twice
    if (document.getElementById("retakeai-toast")) return;

    const parts = [];
    if (codes.authCode) parts.push("Auth Code");
    if (codes.matchToken) parts.push("Match Token");
    if (parts.length === 0) return;

    const toast = document.createElement("div");
    toast.id = "retakeai-toast";
    toast.innerHTML = `
      <div style="
        position: fixed;
        bottom: 24px;
        right: 24px;
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
        animation: retakeai-slide-in 0.4s cubic-bezier(0.16, 1, 0.3, 1);
      ">
        <div style="
          width: 36px; height: 36px;
          border-radius: 10px;
          background: rgba(213, 255, 76, 0.12);
          display: flex; align-items: center; justify-content: center;
        ">
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#d5ff4c" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
            <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/>
            <polyline points="22 4 12 14.01 9 11.01"/>
          </svg>
        </div>
        <div>
          <div style="font-size: 13px; font-weight: 700; color: #fff; letter-spacing: 0.01em;">
            RetakeAI Captured!
          </div>
          <div style="font-size: 11px; color: rgba(255,255,255,0.5); margin-top: 2px;">
            ${parts.join(" & ")} extracted
          </div>
        </div>
      </div>
    `;

    const style = document.createElement("style");
    style.textContent = `
      @keyframes retakeai-slide-in {
        from { opacity: 0; transform: translateY(20px) scale(0.95); }
        to { opacity: 1; transform: translateY(0) scale(1); }
      }
    `;
    document.head.appendChild(style);
    document.body.appendChild(toast);

    // Auto-remove after 6 seconds
    setTimeout(() => {
      toast.style.transition = "opacity 0.4s, transform 0.4s";
      toast.style.opacity = "0";
      toast.style.transform = "translateY(10px)";
      setTimeout(() => toast.remove(), 400);
    }, 6000);
  }

  // Initial scan
  const codes = scanPage();
  if (codes.authCode || codes.matchToken) {
    storeAndNotify(codes);
  }

  // Re-scan when DOM changes (Steam page loads data lazily)
  const observer = new MutationObserver(() => {
    const freshCodes = scanPage();
    if (freshCodes.authCode || freshCodes.matchToken) {
      storeAndNotify(freshCodes);
      observer.disconnect();
    }
  });

  observer.observe(document.body, {
    childList: true,
    subtree: true,
    characterData: true,
  });

  // Stop observing after 30 seconds to avoid overhead
  setTimeout(() => observer.disconnect(), 30000);
})();
