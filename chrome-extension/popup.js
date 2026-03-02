/**
 * RetakeAI – Popup script
 * Reads stored codes, updates UI, and sends codes to RetakeAI API.
 */

// RetakeAI base URL — change this if deployed
const RETAKEAI_BASE = "http://localhost:3000";

document.addEventListener("DOMContentLoaded", () => {
  const authValue = document.getElementById("auth-value");
  const tokenValue = document.getElementById("token-value");
  const authCard = document.getElementById("auth-card");
  const tokenCard = document.getElementById("token-card");
  const timestampEl = document.getElementById("timestamp");
  const sendBtn = document.getElementById("btn-send-retakeai");
  const statusSection = document.getElementById("status-section");
  const statusMessage = document.getElementById("status-message");

  let storedAuthCode = null;
  let storedMatchToken = null;

  // Load stored codes
  chrome.storage.local.get(
    ["retakeai_auth_code", "retakeai_match_token", "retakeai_extracted_at"],
    (data) => {
      if (data.retakeai_auth_code) {
        storedAuthCode = data.retakeai_auth_code;
        authValue.textContent = storedAuthCode;
        authCard.classList.add("has-value");
      }
      if (data.retakeai_match_token) {
        storedMatchToken = data.retakeai_match_token;
        tokenValue.textContent = storedMatchToken;
        tokenCard.classList.add("has-value");
      }
      if (data.retakeai_extracted_at) {
        const d = new Date(data.retakeai_extracted_at);
        timestampEl.textContent = `Captured ${d.toLocaleDateString()} ${d.toLocaleTimeString()}`;
      }

      // Enable send button if we have at least one code
      if (storedAuthCode || storedMatchToken) {
        sendBtn.disabled = false;
      }
    }
  );

  // Send codes to RetakeAI API
  sendBtn.addEventListener("click", async () => {
    if (!storedAuthCode && !storedMatchToken) return;

    sendBtn.disabled = true;
    sendBtn.textContent = "Sending...";
    showStatus("Connecting to RetakeAI...", "info");

    try {
      const payload = {};
      if (storedAuthCode) payload.auth_code = storedAuthCode;
      if (storedMatchToken) payload.last_known_match_code = storedMatchToken;

      const res = await fetch(`${RETAKEAI_BASE}/api/extension/save-codes`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify(payload),
      });

      const data = await res.json();

      if (res.ok && data.success) {
        showStatus("Codes saved to RetakeAI!", "success");
        sendBtn.textContent = "Sent!";
        // Auto-open dashboard after success
        setTimeout(() => {
          chrome.tabs.create({ url: `${RETAKEAI_BASE}/dashboard` });
          window.close();
        }, 1500);
      } else if (res.status === 401) {
        showStatus(
          "Not signed in to RetakeAI. Please log in first, then try again.",
          "error"
        );
        resetSendBtn();
      } else {
        showStatus(data.error || "Failed to save. Try again.", "error");
        resetSendBtn("Retry Send");
      }
    } catch (err) {
      console.error("Send failed:", err);
      showStatus(
        "Could not reach RetakeAI. Make sure the app is running.",
        "error"
      );
      resetSendBtn("Retry Send");
    }
  });

  function resetSendBtn(label) {
    sendBtn.disabled = false;
    sendBtn.innerHTML = `
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <line x1="22" y1="2" x2="11" y2="13"/>
        <polygon points="22 2 15 22 11 13 2 9 22 2"/>
      </svg>
      ${label || "Send Codes to RetakeAI"}
    `;
  }

  // Open Steam GCPD page
  document.getElementById("btn-open-steam").addEventListener("click", () => {
    chrome.tabs.create({
      url: "https://help.steampowered.com/en/wizard/HelpWithGameIssue/?appid=730&issueid=128",
    });
    window.close();
  });

  // Clear stored codes
  document.getElementById("btn-clear").addEventListener("click", () => {
    chrome.runtime.sendMessage({ type: "CLEAR_CODES" }, () => {
      storedAuthCode = null;
      storedMatchToken = null;
      authValue.textContent = "\u2014";
      tokenValue.textContent = "\u2014";
      authCard.classList.remove("has-value");
      tokenCard.classList.remove("has-value");
      timestampEl.textContent = "";
      sendBtn.disabled = true;
      showStatus("Codes cleared.", "info");
    });
  });

  // Listen for live updates from content script
  chrome.runtime.onMessage.addListener((msg) => {
    if (msg.type === "CODES_EXTRACTED") {
      if (msg.authCode) {
        storedAuthCode = msg.authCode;
        authValue.textContent = msg.authCode;
        authCard.classList.add("has-value");
      }
      if (msg.matchToken) {
        storedMatchToken = msg.matchToken;
        tokenValue.textContent = msg.matchToken;
        tokenCard.classList.add("has-value");
      }
      timestampEl.textContent = "Captured just now";
      sendBtn.disabled = false;
    }
  });

  function showStatus(text, type) {
    statusSection.style.display = "block";
    statusMessage.textContent = text;
    statusMessage.className = "status-msg status-" + type;
  }
});
