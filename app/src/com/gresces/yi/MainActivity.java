package com.gresces.yi;

import android.Manifest;
import android.app.Activity;
import android.app.AlertDialog;
import android.content.ActivityNotFoundException;
import android.content.Intent;
import android.content.SharedPreferences;
import android.content.pm.PackageManager;
import android.graphics.Color;
import android.net.Uri;
import android.os.Build;
import android.os.Bundle;
import android.text.InputType;
import android.text.TextUtils;
import android.view.Gravity;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.view.ViewGroup;
import android.webkit.CookieManager;
import android.webkit.WebChromeClient;
import android.webkit.WebResourceError;
import android.webkit.WebResourceRequest;
import android.webkit.WebResourceResponse;
import android.webkit.WebSettings;
import android.webkit.WebView;
import android.webkit.WebViewClient;
import android.widget.CheckBox;
import android.widget.EditText;
import android.widget.FrameLayout;
import android.widget.LinearLayout;
import android.widget.ProgressBar;
import android.widget.ScrollView;
import android.widget.TextView;
import android.widget.Toast;

public final class MainActivity extends Activity {
    private static final String PREFS = "yi";
    private static final String KEY_URL = "base_url";
    private static final String DEFAULT_URL = "http://120.53.234.103:9093/";
    private static final String DEFAULT_SERVICE_URL = "http://120.53.234.103:9092/";
    private static final String EXTRA_URL = "yi_url";

    private static final int MENU_RELOAD = 1;
    private static final int MENU_SERVER = 2;
    private static final int MENU_BROWSER = 3;
    private static final int MENU_WATCH = 4;
    private static final int REQUEST_NOTIFICATIONS = 7;

    private WebView web;
    private ProgressBar progress;
    private String baseUrl;
    private boolean pendingWatchStart;

    @Override
    protected void onCreate(Bundle savedState) {
        super.onCreate(savedState);

        SharedPreferences prefs = getSharedPreferences(PREFS, MODE_PRIVATE);
        baseUrl = normalizeUrl(prefs.getString(KEY_URL, DEFAULT_URL));
        if (baseUrl == null) {
            baseUrl = DEFAULT_URL;
        }

        Intent intent = getIntent();
        String extraUrl = normalizeUrl(intent == null ? null : intent.getStringExtra(EXTRA_URL));
        if (extraUrl != null) {
            baseUrl = extraUrl;
            prefs.edit().putString(KEY_URL, baseUrl).apply();
        }

        buildUi();
        if (getActionBar() != null) {
            getActionBar().setTitle("Yi");
        }
        updateSubtitle();

        if (extraUrl == null && savedState != null && web.restoreState(savedState) != null) {
            startWatchIfEnabled();
            return;
        }
        web.loadUrl(baseUrl);
        startWatchIfEnabled();
    }

    private void startWatchIfEnabled() {
        if (getSharedPreferences(PREFS, MODE_PRIVATE)
                .getBoolean(YiWatchService.KEY_ENABLED, false)) {
            startWatchService();
        }
    }

    private void buildUi() {
        FrameLayout root = new FrameLayout(this);

        web = new WebView(this);
        root.addView(web, new FrameLayout.LayoutParams(
                ViewGroup.LayoutParams.MATCH_PARENT,
                ViewGroup.LayoutParams.MATCH_PARENT));

        progress = new ProgressBar(this, null, android.R.attr.progressBarStyleHorizontal);
        progress.setMax(100);
        FrameLayout.LayoutParams progressParams = new FrameLayout.LayoutParams(
                ViewGroup.LayoutParams.MATCH_PARENT, dp(3));
        progressParams.gravity = Gravity.TOP;
        root.addView(progress, progressParams);

        setContentView(root);
        configureWebView();
    }

    private void configureWebView() {
        WebSettings settings = web.getSettings();
        settings.setJavaScriptEnabled(true);
        settings.setDomStorageEnabled(true);
        settings.setDatabaseEnabled(true);
        settings.setLoadWithOverviewMode(true);
        settings.setUseWideViewPort(true);
        settings.setAllowFileAccess(false);
        settings.setAllowContentAccess(false);
        settings.setMediaPlaybackRequiresUserGesture(false);
        settings.setMixedContentMode(WebSettings.MIXED_CONTENT_ALWAYS_ALLOW);
        settings.setCacheMode(WebSettings.LOAD_DEFAULT);
        settings.setUserAgentString(settings.getUserAgentString() + " YiAndroid/0.1.5");
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            settings.setSafeBrowsingEnabled(true);
        }

        web.setBackgroundColor(Color.WHITE);
        CookieManager cookies = CookieManager.getInstance();
        cookies.setAcceptCookie(true);
        cookies.setAcceptThirdPartyCookies(web, true);

        web.setWebViewClient(new WebViewClient() {
            @Override
            public boolean shouldOverrideUrlLoading(WebView view, WebResourceRequest request) {
                return openInWebViewOrBrowser(request.getUrl());
            }

            @Override
            @SuppressWarnings("deprecation")
            public boolean shouldOverrideUrlLoading(WebView view, String url) {
                return openInWebViewOrBrowser(Uri.parse(url));
            }

            @Override
            public void onPageStarted(WebView view, String url, android.graphics.Bitmap favicon) {
                progress.setVisibility(View.VISIBLE);
            }

            @Override
            public void onPageFinished(WebView view, String url) {
                progress.setVisibility(View.GONE);
                updateSubtitle();
            }

            @Override
            public void onReceivedError(WebView view, WebResourceRequest request,
                                        WebResourceError error) {
                if (!request.isForMainFrame()) {
                    return;
                }
                CharSequence description = error == null ? null : error.getDescription();
                showLoadError(description == null ? "" : description.toString());
            }

            @Override
            public void onReceivedHttpError(WebView view, WebResourceRequest request,
                                            WebResourceResponse response) {
                if (!request.isForMainFrame() || response == null) {
                    return;
                }
                if (response.getStatusCode() >= 500) {
                    showLoadError("HTTP " + response.getStatusCode());
                }
            }
        });

        web.setWebChromeClient(new WebChromeClient() {
            @Override
            public void onProgressChanged(WebView view, int newProgress) {
                progress.setProgress(newProgress);
                progress.setVisibility(newProgress >= 100 ? View.GONE : View.VISIBLE);
            }
        });
    }

    private boolean openInWebViewOrBrowser(Uri uri) {
        if (uri == null) {
            return false;
        }
        String scheme = uri.getScheme();
        if (scheme == null) {
            return false;
        }
        scheme = scheme.toLowerCase();
        if ("about".equals(scheme) || "blob".equals(scheme) ||
                "data".equals(scheme) || "javascript".equals(scheme)) {
            return false;
        }
        if (("http".equals(scheme) || "https".equals(scheme)) && isAppOrigin(uri)) {
            return false;
        }
        try {
            startActivity(new Intent(Intent.ACTION_VIEW, uri));
        } catch (ActivityNotFoundException ignored) {
            Toast.makeText(this, "没有可以打开这个链接的应用", Toast.LENGTH_SHORT).show();
        }
        return true;
    }

    private boolean isAppOrigin(Uri uri) {
        Uri base = Uri.parse(baseUrl);
        if (base.getScheme() == null || uri.getScheme() == null) {
            return false;
        }
        if (!base.getScheme().equalsIgnoreCase(uri.getScheme())) {
            return false;
        }
        if (base.getHost() == null || !base.getHost().equalsIgnoreCase(uri.getHost())) {
            return false;
        }
        return effectivePort(base) == effectivePort(uri);
    }

    private static int effectivePort(Uri uri) {
        if (uri.getPort() >= 0) {
            return uri.getPort();
        }
        return "https".equalsIgnoreCase(uri.getScheme()) ? 443 : 80;
    }

    private void updateSubtitle() {
        if (getActionBar() == null) {
            return;
        }
        Uri uri = Uri.parse(baseUrl);
        String host = uri.getHost();
        if (TextUtils.isEmpty(host)) {
            getActionBar().setSubtitle(baseUrl);
            return;
        }
        int port = uri.getPort();
        getActionBar().setSubtitle(port > 0 ? host + ":" + port : host);
    }

    private void showServerDialog() {
        final EditText input = new EditText(this);
        input.setSingleLine(true);
        input.setInputType(InputType.TYPE_CLASS_TEXT | InputType.TYPE_TEXT_VARIATION_URI);
        input.setText(baseUrl);
        input.setSelection(input.getText().length());

        FrameLayout box = new FrameLayout(this);
        int padding = dp(20);
        box.setPadding(padding, padding / 2, padding, 0);
        box.addView(input, new FrameLayout.LayoutParams(
                ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT));

        new AlertDialog.Builder(this)
                .setTitle("Yi 服务器")
                .setMessage("输入 Yi 前端地址，例如 http://192.168.1.10:9093/")
                .setView(box)
                .setPositiveButton("保存", (dialog, which) -> {
                    String normalized = normalizeUrl(input.getText().toString());
                    if (normalized == null) {
                        Toast.makeText(this, "地址无效，需要 http:// 或 https://", Toast.LENGTH_LONG).show();
                        return;
                    }
                    baseUrl = normalized;
                    getSharedPreferences(PREFS, MODE_PRIVATE).edit()
                            .putString(KEY_URL, baseUrl).apply();
                    updateSubtitle();
                    web.loadUrl(baseUrl);
                })
                .setNeutralButton("恢复默认", (dialog, which) -> {
                    baseUrl = DEFAULT_URL;
                    getSharedPreferences(PREFS, MODE_PRIVATE).edit()
                            .putString(KEY_URL, baseUrl).apply();
                    updateSubtitle();
                    web.loadUrl(baseUrl);
                })
                .setNegativeButton("取消", null)
                .show();
    }

    private void showWatchDialog() {
        SharedPreferences prefs = getSharedPreferences(PREFS, MODE_PRIVATE);

        LinearLayout form = new LinearLayout(this);
        form.setOrientation(LinearLayout.VERTICAL);
        int padding = dp(20);
        form.setPadding(padding, dp(8), padding, 0);

        EditText serviceUrl = field(prefs.getString(
                YiWatchService.KEY_SERVICE_URL, DEFAULT_SERVICE_URL),
                "http://服务器:9092/", false, true);
        EditText token = field(prefs.getString(YiWatchService.KEY_TOKEN, ""),
                "认证 token，留空表示未开启认证", true, false);
        EditText types = field(prefs.getString(YiWatchService.KEY_TYPES, ""),
                "事件类型，逗号分隔；留空 = 全部类型", false, false);
        EditText keywords = field(prefs.getString(YiWatchService.KEY_KEYWORDS, ""),
                "关键词，逗号分隔；留空 = 不过滤", false, false);
        CheckBox enabled = new CheckBox(this);
        enabled.setText("后台常驻，并在匹配时通知");
        enabled.setChecked(prefs.getBoolean(YiWatchService.KEY_ENABLED, false));

        addLabel(form, "服务端口地址");
        form.addView(serviceUrl);
        addLabel(form, "认证 token");
        form.addView(token);
        addLabel(form, "主题类型");
        form.addView(types);
        addLabel(form, "内容关键词");
        form.addView(keywords);
        form.addView(enabled);

        TextView note = new TextView(this);
        note.setTextSize(12);
        note.setTextColor(Color.parseColor("#6b7280"));
        note.setPadding(0, dp(10), 0, 0);
        note.setText("类型和关键词同时填写时需要同时匹配；留空表示不过滤。"
                + "首次启用从当前最新事件开始，不补发历史。");
        form.addView(note);

        ScrollView scroll = new ScrollView(this);
        scroll.addView(form);

        new AlertDialog.Builder(this)
                .setTitle("后台主题提醒")
                .setView(scroll)
                .setPositiveButton("保存", (dialog, which) -> {
                    String normalized = normalizeUrl(serviceUrl.getText().toString());
                    if (normalized == null) {
                        Toast.makeText(this, "服务地址无效，需要 http:// 或 https://",
                                Toast.LENGTH_LONG).show();
                        return;
                    }

                    String oldServiceUrl = prefs.getString(
                            YiWatchService.KEY_SERVICE_URL, DEFAULT_SERVICE_URL);
                    SharedPreferences.Editor editor = prefs.edit()
                            .putString(YiWatchService.KEY_SERVICE_URL, normalized)
                            .putString(YiWatchService.KEY_TOKEN,
                                    token.getText().toString().trim())
                            .putString(YiWatchService.KEY_TYPES,
                                    types.getText().toString().trim())
                            .putString(YiWatchService.KEY_KEYWORDS,
                                    keywords.getText().toString().trim())
                            .putBoolean(YiWatchService.KEY_ENABLED, enabled.isChecked());
                    if (!normalized.equals(oldServiceUrl)) {
                        editor.putBoolean(YiWatchService.KEY_PRIMED, false)
                                .remove(YiWatchService.KEY_SEEN);
                    }
                    editor.apply();

                    if (enabled.isChecked()) {
                        startWatchWithPermission();
                    } else {
                        stopWatchService();
                    }
                })
                .setNegativeButton("取消", null)
                .show();
    }

    private EditText field(String value, String hint, boolean password, boolean uri) {
        EditText input = new EditText(this);
        input.setSingleLine(true);
        input.setText(value);
        input.setHint(hint);
        int variation = 0;
        if (password) {
            variation = InputType.TYPE_TEXT_VARIATION_PASSWORD;
        } else if (uri) {
            variation = InputType.TYPE_TEXT_VARIATION_URI;
        }
        input.setInputType(InputType.TYPE_CLASS_TEXT | variation);
        input.setSelection(input.getText().length());
        LinearLayout.LayoutParams params = new LinearLayout.LayoutParams(
                ViewGroup.LayoutParams.MATCH_PARENT,
                ViewGroup.LayoutParams.WRAP_CONTENT);
        params.topMargin = dp(2);
        input.setLayoutParams(params);
        return input;
    }

    private void addLabel(LinearLayout parent, String text) {
        TextView label = new TextView(this);
        label.setText(text);
        label.setTextSize(12);
        label.setTextColor(Color.parseColor("#6b7280"));
        label.setPadding(0, dp(12), 0, 0);
        parent.addView(label);
    }

    private void startWatchWithPermission() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU
                && checkSelfPermission(Manifest.permission.POST_NOTIFICATIONS)
                != PackageManager.PERMISSION_GRANTED) {
            pendingWatchStart = true;
            requestPermissions(
                    new String[]{Manifest.permission.POST_NOTIFICATIONS},
                    REQUEST_NOTIFICATIONS);
            return;
        }
        startWatchService();
    }

    private void startWatchService() {
        try {
            Intent service = new Intent(this, YiWatchService.class);
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                startForegroundService(service);
            } else {
                startService(service);
            }
        } catch (RuntimeException error) {
            Toast.makeText(this, "无法启动后台监听: " + error.getMessage(),
                    Toast.LENGTH_LONG).show();
        }
    }

    private void stopWatchService() {
        stopService(new Intent(this, YiWatchService.class));
    }

    @Override
    public void onRequestPermissionsResult(int requestCode, String[] permissions,
                                           int[] grantResults) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults);
        if (requestCode != REQUEST_NOTIFICATIONS || !pendingWatchStart) {
            return;
        }
        pendingWatchStart = false;
        startWatchService();
        if (grantResults.length == 0
                || grantResults[0] != PackageManager.PERMISSION_GRANTED) {
            Toast.makeText(this, "通知权限未授予，后台监听仍会运行，但提醒可能不可见。",
                    Toast.LENGTH_LONG).show();
        }
    }

    private static String normalizeUrl(String raw) {
        if (raw == null) {
            return null;
        }
        String value = raw.trim();
        if (value.isEmpty()) {
            return null;
        }
        if (!value.matches("(?i)^https?://.*")) {
            if (value.matches("^[a-zA-Z][a-zA-Z0-9+.-]*://.*")) {
                return null;
            }
            value = "http://" + value;
        }
        Uri uri = Uri.parse(value);
        String scheme = uri.getScheme();
        if (scheme == null ||
                (!"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme))) {
            return null;
        }
        if (TextUtils.isEmpty(uri.getHost())) {
            return null;
        }
        if (!value.endsWith("/")) {
            value += "/";
        }
        return value;
    }

    private void showLoadError(String description) {
        String detail = TextUtils.isEmpty(description) ? "无法连接到服务器" : description;
        String html = "<!doctype html><html><head>"
                + "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">"
                + "<style>"
                + "body{margin:0;min-height:100vh;display:flex;align-items:center;"
                + "justify-content:center;padding:24px;box-sizing:border-box;"
                + "background:#f9fafb;color:#111827;font-family:system-ui,sans-serif}"
                + ".card{width:100%;max-width:420px;padding:28px;background:#fff;"
                + "border:1px solid #e5e7eb;border-radius:12px;"
                + "box-shadow:0 1px 2px rgba(17,24,39,.06)}"
                + "h1{margin:0 0 10px;font-size:20px}p{margin:0 0 16px;color:#6b7280;"
                + "line-height:1.6;word-break:break-word}"
                + "a{display:inline-block;padding:10px 16px;border-radius:8px;"
                + "background:#6366f1;color:#fff;text-decoration:none;font-weight:600}"
                + ".hint{margin:16px 0 0;font-size:12px;color:#9ca3af}"
                + "</style></head><body><div class=\"card\"><h1>连不上 Yi</h1><p>"
                + TextUtils.htmlEncode(detail)
                + "</p><a href=\"" + TextUtils.htmlEncode(baseUrl) + "\">重试</a>"
                + "<p class=\"hint\">检查服务地址；右上角菜单可以修改服务器。</p>"
                + "</div></body></html>";
        web.loadDataWithBaseURL(baseUrl, html, "text/html", "UTF-8", null);
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        menu.add(Menu.NONE, MENU_RELOAD, 1, "刷新");
        menu.add(Menu.NONE, MENU_SERVER, 2, "服务器");
        menu.add(Menu.NONE, MENU_WATCH, 3, "后台提醒");
        menu.add(Menu.NONE, MENU_BROWSER, 4, "浏览器打开");
        return true;
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        switch (item.getItemId()) {
            case MENU_RELOAD:
                web.loadUrl(baseUrl);
                return true;
            case MENU_SERVER:
                showServerDialog();
                return true;
            case MENU_WATCH:
                showWatchDialog();
                return true;
            case MENU_BROWSER:
                try {
                    startActivity(new Intent(Intent.ACTION_VIEW,
                            Uri.parse(web.getUrl() == null ? baseUrl : web.getUrl())));
                } catch (ActivityNotFoundException ignored) {
                    Toast.makeText(this, "没有可以打开浏览器的应用", Toast.LENGTH_SHORT).show();
                }
                return true;
            default:
                return super.onOptionsItemSelected(item);
        }
    }

    @Override
    @SuppressWarnings("deprecation")
    public void onBackPressed() {
        if (web != null && web.canGoBack()) {
            web.goBack();
            return;
        }
        super.onBackPressed();
    }

    @Override
    protected void onSaveInstanceState(Bundle outState) {
        super.onSaveInstanceState(outState);
        if (web != null) {
            web.saveState(outState);
        }
    }

    @Override
    protected void onPause() {
        if (web != null) {
            web.onPause();
        }
        super.onPause();
    }

    @Override
    protected void onResume() {
        super.onResume();
        if (web != null) {
            web.onResume();
        }
    }

    @Override
    protected void onDestroy() {
        if (web != null) {
            web.destroy();
            web = null;
        }
        super.onDestroy();
    }

    private int dp(int value) {
        return Math.round(value * getResources().getDisplayMetrics().density);
    }
}
