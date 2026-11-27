package uz.itkoders.neuron

import android.annotation.SuppressLint
import android.app.Activity
import android.content.Intent
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.webkit.JavascriptInterface
import android.webkit.WebChromeClient
import android.webkit.WebResourceRequest
import android.webkit.WebView
import android.webkit.WebViewClient
import android.webkit.ValueCallback
import java.net.HttpURLConnection
import java.net.URL
import java.util.concurrent.Executors

class MainActivity : Activity() {
    private val executor = Executors.newSingleThreadExecutor()
    private lateinit var tokenStore: SecureTokenStore
    private var webView: WebView? = null
    private var fileCallback: ValueCallback<Array<Uri>>? = null
    private val filePickerRequest = 7104

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        tokenStore = SecureTokenStore(this)
        openMiniApp()
    }

    private inner class SessionBridge {
        @JavascriptInterface
        fun getSessionToken(): String = tokenStore.read().orEmpty()

        // Only a server-issued token returned by /api/android/login is passed here.
        @JavascriptInterface
        fun saveSessionToken(token: String): Boolean {
            if (token.length !in 40..512) return false
            return try {
                tokenStore.save(token)
                true
            } catch (_: Exception) {
                false
            }
        }

        @JavascriptInterface
        fun logout() {
            val oldToken = tokenStore.read()
            executor.execute {
                var connection: HttpURLConnection? = null
                try {
                    if (!oldToken.isNullOrBlank()) {
                        connection = (URL(BuildConfig.NEURON_BASE_URL.trimEnd('/') + "/api/android/logout")
                            .openConnection() as HttpURLConnection).apply {
                            requestMethod = "POST"
                            connectTimeout = 5000
                            readTimeout = 5000
                            setRequestProperty("Authorization", "Bearer " + oldToken)
                            setRequestProperty("Content-Type", "application/json")
                            doOutput = true
                        }
                        connection.outputStream.use { it.write("{}".toByteArray(Charsets.UTF_8)) }
                        connection.responseCode
                    }
                } catch (_: Exception) {
                    // Local logout still proceeds if the device is offline.
                } finally {
                    connection?.disconnect()
                    tokenStore.clear()
                    runOnUiThread { webView?.reload() }
                }
            }
        }
    }

    @SuppressLint("SetJavaScriptEnabled")
    private fun openMiniApp() {
        val base = BuildConfig.NEURON_BASE_URL.trimEnd('/')
        val baseUri = Uri.parse(base)
        val browser = WebView(this).apply {
            settings.javaScriptEnabled = true
            settings.domStorageEnabled = true
            settings.allowFileAccess = false
            settings.allowContentAccess = false
            settings.javaScriptCanOpenWindowsAutomatically = false
            settings.setSupportMultipleWindows(false)
            settings.mixedContentMode = android.webkit.WebSettings.MIXED_CONTENT_NEVER_ALLOW
            settings.useWideViewPort = true
            settings.loadWithOverviewMode = false
            settings.setSupportZoom(false)
            if (Build.VERSION.SDK_INT >= 26) settings.safeBrowsingEnabled = true
            addJavascriptInterface(SessionBridge(), "NeuronAndroid")
            webChromeClient = object : WebChromeClient() {
                override fun onShowFileChooser(
                    view: WebView?,
                    callback: ValueCallback<Array<Uri>>?,
                    params: FileChooserParams?
                ): Boolean {
                    fileCallback?.onReceiveValue(null)
                    fileCallback = callback
                    val intent = Intent(Intent.ACTION_GET_CONTENT).apply {
                        addCategory(Intent.CATEGORY_OPENABLE)
                        type = "*/*"
                    }
                    return try {
                        startActivityForResult(Intent.createChooser(intent, "Faylni tanlang"), filePickerRequest)
                        true
                    } catch (_: Exception) {
                        fileCallback = null
                        false
                    }
                }
            }
            webViewClient = object : WebViewClient() {
                override fun shouldOverrideUrlLoading(view: WebView, request: WebResourceRequest): Boolean {
                    val uri = request.url
                    val uriPort = if (uri.port == -1) 443 else uri.port
                    val basePort = if (baseUri.port == -1) 443 else baseUri.port
                    val sameOrigin = uri.scheme == "https" &&
                        uri.host.equals(baseUri.host, ignoreCase = true) &&
                        uriPort == basePort
                    if (sameOrigin) return false
                    if (uri.scheme == "https" || uri.scheme == "tel" || uri.scheme == "mailto") {
                        try { startActivity(Intent(Intent.ACTION_VIEW, uri)) } catch (_: Exception) {}
                    }
                    return true
                }
            }
            loadUrl("$base/app?platform=android&role=entry")
        }
        webView = browser
        setContentView(browser)
    }

    @Deprecated("Deprecated in Android")
    override fun onActivityResult(requestCode: Int, resultCode: Int, data: Intent?) {
        super.onActivityResult(requestCode, resultCode, data)
        if (requestCode == filePickerRequest) {
            val result = if (resultCode == RESULT_OK && data?.data != null) arrayOf(data.data!!) else null
            fileCallback?.onReceiveValue(result)
            fileCallback = null
        }
    }

    @Deprecated("Deprecated in Android")
    override fun onBackPressed() {
        val browser = webView
        if (browser != null && browser.canGoBack()) browser.goBack() else super.onBackPressed()
    }

    override fun onDestroy() {
        fileCallback?.onReceiveValue(null)
        webView?.destroy()
        executor.shutdown()
        super.onDestroy()
    }
}
