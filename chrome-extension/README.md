# RetakeAI – CS2 Auth Linker (Chrome Extension)

A Chrome extension that automatically extracts your **Game Authentication Code** and **Last Match Sharing Code** from the Steam GCPD page and links them to your RetakeAI account — no manual copy-pasting needed.

## How It Works

1. **Install the extension** → it opens the Steam GCPD page automatically
2. **Log in to Steam** → the extension detects and captures your auth code + match token
3. **Click "Send Codes to RetakeAI"** → saves directly to your account (or auto-fills the onboarding form)

## Features

- **Auto-extraction**: Runs a content script on `help.steampowered.com` that scans for auth codes and match tokens using regex pattern matching
- **Auto-fill**: When you visit RetakeAI's onboarding page, codes are pre-filled automatically
- **One-click save**: Send codes directly to RetakeAI via the API from the popup
- **Visual toast**: Shows a confirmation on the Steam page when codes are captured
- **Secure**: Codes are stored locally in Chrome storage and sent only to your RetakeAI instance

## Installation (Developer Mode)

1. Open Chrome and go to `chrome://extensions/`
2. Enable **Developer mode** (toggle in top-right)
3. Click **Load unpacked**
4. Select the `chrome-extension/` folder from this repo
5. The extension icon appears in your toolbar

## File Structure

```
chrome-extension/
├── manifest.json          # Extension manifest (Manifest V3)
├── background.js          # Service worker — handles messaging
├── content-steam.js       # Content script — extracts codes from Steam GCPD
├── content-retakeai.js    # Content script — auto-fills RetakeAI onboarding
├── popup.html             # Extension popup UI
├── popup.js               # Popup logic + API calls
├── popup.css              # Popup styles (matches RetakeAI dark theme)
├── icons/                 # Extension icons (16/32/48/128px)
└── README.md              # This file
```

## API Endpoint

The extension communicates with:

```
POST /api/extension/save-codes
Body: { "auth_code": "XXXX-XXXXX-XXXX", "last_known_match_code": "CSGO-..." }
```

Requires the user to be authenticated (Steam session cookie).

## Configuration

By default, the extension targets `http://localhost:3000`. To change this for a deployed instance, edit the `RETAKEAI_BASE` constant in `popup.js` and update `content_scripts.matches` in `manifest.json`.

## Permissions

| Permission | Reason |
|---|---|
| `storage` | Store extracted codes locally |
| `activeTab` | Access the current tab |
| `host_permissions: help.steampowered.com` | Run content script on Steam GCPD page |
