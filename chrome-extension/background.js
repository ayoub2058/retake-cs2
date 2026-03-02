/**
 * RetakeAI – Background service worker
 * Handles messages between content scripts and popup.
 */

// Listen for codes extracted from Steam
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.type === "CODES_EXTRACTED") {
    // Update badge to show codes are ready
    chrome.action.setBadgeText({ text: "✓" });
    chrome.action.setBadgeBackgroundColor({ color: "#d5ff4c" });

    // Clear badge after 30 seconds
    setTimeout(() => {
      chrome.action.setBadgeText({ text: "" });
    }, 30000);
  }

  if (message.type === "GET_CODES") {
    chrome.storage.local.get(
      ["retakeai_auth_code", "retakeai_match_token", "retakeai_extracted_at"],
      (data) => {
        sendResponse(data);
      }
    );
    return true; // async response
  }

  if (message.type === "CLEAR_CODES") {
    chrome.storage.local.remove(
      ["retakeai_auth_code", "retakeai_match_token", "retakeai_extracted_at"],
      () => {
        chrome.action.setBadgeText({ text: "" });
        sendResponse({ success: true });
      }
    );
    return true;
  }
});

// When extension is installed, open Steam GCPD page in new tab
chrome.runtime.onInstalled.addListener((details) => {
  if (details.reason === "install") {
    chrome.tabs.create({
      url: "https://help.steampowered.com/en/wizard/HelpWithGameIssue/?appid=730&issueid=128",
    });
  }
});
