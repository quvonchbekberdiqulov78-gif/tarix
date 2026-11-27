# NEURON Android

Android client for the existing NEURON service. All Android work stays on the isolated `android-apk` branch until reviewed and tested. The existing Telegram Mini App remains available at `/app` and keeps Telegram's signed `initData` authentication path.

## Authentication design

1. In a private chat with the NEURON Telegram bot, the user sends `/apk`.
2. The bot issues a random 10-digit code valid for 5 minutes.
3. The code is stored only as a peppered SHA-256 hash and is consumed atomically once.
4. The Android app exchanges it at `POST /api/android/login`.
5. The server returns a random bearer session token with a 30-day expiry. Only its hash is stored server-side.
6. The app encrypts the token using a non-exportable AES-GCM key in Android Keystore.
7. The Mini App's normal API requests carry the bearer token only when opened inside the Android app. Telegram requests continue to use signed Telegram initData.
8. Android teacher registration uses the existing Telegram self-contact verification: after submitting the form in the APK, send `/sharecontact` to the same bot chat and tap its own-contact button. The bot checks both Telegram ownership and the submitted phone number.

The backend validates the Telegram user ID against the existing role/account tables for every protected request. A client cannot choose or grant itself a teacher, student, parent, or admin role.

## Security

- HTTPS-only server URL; Android cleartext traffic is disabled.
- WebView safe browsing enabled, mixed content disabled, local file access disabled, and navigation restricted to the configured HTTPS host.
- Only the Internet permission is declared. No SMS, contacts, accessibility, overlay, or package-install permissions.
- The system file picker is used for user-selected uploads; no broad storage permission is requested.
- Session token is encrypted at rest with Android Keystore. Logout attempts to revoke the server session and always clears local credentials.
- The one-time code must never be shared with anyone. If it expires, request a new one with `/apk`.

No APK can be guaranteed to pass every third-party antivirus or Play Protect decision. This project avoids suspicious permissions and behavior, uses standard Android APIs, and must be built and distributed from a trusted source. Do not download repackaged APKs from unknown sites.

## Build from GitHub Actions

Every push to the isolated `android-apk` branch automatically starts a debug APK build and stores its artifact for 14 days. For a manual build, open **Actions → Build NEURON Android APK → Run workflow** on the `android-apk` branch.

- Enter only the Render HTTPS root URL, e.g. `https://your-service.onrender.com` (no `/app` suffix).
- Choose `debug` for a first installation/test. Debug builds are not suitable for seamless future updates because CI debug signing is not a permanent release identity.
- Choose `release` only after setting up the permanent signing secrets below.

### Permanent release signing (required for in-place updates)

Create one Android signing keystore and keep a secure backup. Never commit it to GitHub or send it in chat. Add these repository Actions secrets:

- `ANDROID_KEYSTORE_BASE64`: base64-encoded keystore file
- `ANDROID_KEYSTORE_PASSWORD`
- `ANDROID_KEY_ALIAS`
- `ANDROID_KEY_PASSWORD`

Every release must use the exact same keystore and alias. If the key is lost or changed, Android will not install the new APK over the existing signed app. The workflow uses an increasing version code for each build.

## Backend deployment

The Android auth changes in this branch require a Render deploy of this branch (or a reviewed merge into the service's deployment branch). They do not alter the existing Telegram initData validation. Do not distribute the APK until the Render backend is running this version and the login flow has been tested.

## Current status

This is an in-progress integration. A successful Android build alone does not prove the server login and every Mini App role works. Validate teacher, student, parent, admin, file upload, session expiry, logout, and Telegram Mini App login before production distribution.
