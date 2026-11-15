package com.gresces.yi;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.Service;
import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;
import android.os.Build;
import android.os.IBinder;
import android.os.PowerManager;
import android.text.TextUtils;
import android.util.Log;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Foreground watcher for Yi events.
 *
 * It polls the service API for every event type, filters locally by the
 * configured event types/keywords, and posts Android notifications for new
 * matching events. A foreground service + partial wake lock keeps the poll
 * alive while the app is in the background; this is intentionally visible to
 * the user and can be stopped from the ongoing notification.
 */
public final class YiWatchService extends Service {
    public static final String PREFS = "yi";
    public static final String KEY_ENABLED = "watch_enabled";
    public static final String KEY_SERVICE_URL = "watch_service_url";
    public static final String KEY_TOKEN = "watch_token";
    public static final String KEY_TYPES = "watch_types";
    public static final String KEY_KEYWORDS = "watch_keywords";

    public static final String KEY_SEEN = "watch_seen";
    public static final String KEY_PRIMED = "watch_primed";
    private static final String DEFAULT_SERVICE_URL = "http://120.53.234.103:9092/";

    private static final String ACTION_START = "com.gresces.yi.action.START";
    private static final String ACTION_STOP = "com.gresces.yi.action.STOP";
    private static final String CHANNEL_SERVICE = "yi_service";
    private static final String CHANNEL_ALERTS = "yi_alerts";
    private static final int NOTIFICATION_SERVICE = 1001;
    private static final int NOTIFICATION_EVENT_BASE = 2000;
    private static final int SEEN_LIMIT = 500;
    private static final long POLL_SECONDS = 20;

    private SharedPreferences prefs;
    private NotificationManager notifications;
    private ScheduledExecutorService scheduler;
    private PowerManager.WakeLock wakeLock;
    private final LinkedHashSet<String> seen = new LinkedHashSet<>();

    @Override
    public void onCreate() {
        super.onCreate();
        prefs = getSharedPreferences(PREFS, MODE_PRIVATE);
        notifications = getSystemService(NotificationManager.class);
        loadSeen();
        createChannels();
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        if (intent != null && ACTION_STOP.equals(intent.getAction())) {
            prefs.edit().putBoolean(KEY_ENABLED, false).apply();
            stopSelf();
            return START_NOT_STICKY;
        }

        if (!prefs.getBoolean(KEY_ENABLED, false)) {
            stopSelf();
            return START_NOT_STICKY;
        }

        startForeground(NOTIFICATION_SERVICE, buildServiceNotification());
        acquireWakeLock();
        startScheduler();
        return START_STICKY;
    }

    @Override
    public IBinder onBind(Intent intent) {
        return null;
    }

    @Override
    public void onDestroy() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
        releaseWakeLock();
        stopForeground(true);
        super.onDestroy();
    }

    private void startScheduler() {
        if (scheduler != null) {
            return;
        }
        scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleWithFixedDelay(this::pollSafely, 1, POLL_SECONDS, TimeUnit.SECONDS);
    }

    private void acquireWakeLock() {
        if (wakeLock == null) {
            PowerManager power = (PowerManager) getSystemService(POWER_SERVICE);
            wakeLock = power.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "yi:watch");
            wakeLock.setReferenceCounted(false);
        }
        if (!wakeLock.isHeld()) {
            wakeLock.acquire();
        }
    }

    private void releaseWakeLock() {
        if (wakeLock != null && wakeLock.isHeld()) {
            wakeLock.release();
        }
    }

    private void createChannels() {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.O) {
            return;
        }
        NotificationChannel service = new NotificationChannel(
                CHANNEL_SERVICE, "Yi 后台服务", NotificationManager.IMPORTANCE_LOW);
        service.setDescription("保持 Yi 后台监听");
        notifications.createNotificationChannel(service);

        NotificationChannel alerts = new NotificationChannel(
                CHANNEL_ALERTS, "Yi 主题提醒", NotificationManager.IMPORTANCE_DEFAULT);
        alerts.setDescription("匹配类型或关键词的新事件");
        alerts.enableVibration(true);
        notifications.createNotificationChannel(alerts);
    }

    private Notification.Builder newBuilder(String channel) {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            return new Notification.Builder(this, channel);
        }
        return new Notification.Builder(this);
    }

    private PendingIntent openAppIntent() {
        Intent open = new Intent(this, MainActivity.class);
        open.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK
                | Intent.FLAG_ACTIVITY_CLEAR_TOP
                | Intent.FLAG_ACTIVITY_SINGLE_TOP);
        return PendingIntent.getActivity(this, 0, open,
                PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE);
    }

    private Notification buildServiceNotification() {
        String types = trim(prefs.getString(KEY_TYPES, ""));
        String keywords = trim(prefs.getString(KEY_KEYWORDS, ""));
        String detail = "监听: " + (types.isEmpty() ? "全部类型" : types);
        if (!keywords.isEmpty()) {
            detail += " · 关键词: " + keywords;
        }

        Intent stop = new Intent(this, YiWatchService.class).setAction(ACTION_STOP);
        PendingIntent stopIntent = PendingIntent.getService(this, 1, stop,
                PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE);

        return newBuilder(CHANNEL_SERVICE)
                .setSmallIcon(R.drawable.ic_notification)
                .setContentTitle("Yi 后台监听中")
                .setContentText(detail)
                .setStyle(new Notification.BigTextStyle().bigText(detail))
                .setContentIntent(openAppIntent())
                .setOngoing(true)
                .setCategory(Notification.CATEGORY_SERVICE)
                .addAction(android.R.drawable.ic_menu_close_clear_cancel, "停止", stopIntent)
                .build();
    }

    private void pollSafely() {
        try {
            pollOnce();
        } catch (Exception error) {
            Log.w("YiWatch", "poll failed", error);
        }
    }

    private void pollOnce() throws Exception {
        String serviceUrl = normalizeUrl(prefs.getString(KEY_SERVICE_URL, DEFAULT_SERVICE_URL));
        if (serviceUrl == null) {
            return;
        }
        String token = trim(prefs.getString(KEY_TOKEN, ""));
        String endpoint = serviceUrl + "events?limit=200";

        HttpURLConnection connection = null;
        try {
            connection = (HttpURLConnection) new URL(endpoint).openConnection();
            connection.setConnectTimeout(8000);
            connection.setReadTimeout(10000);
            connection.setRequestProperty("Accept", "application/json");
            if (!token.isEmpty()) {
                connection.setRequestProperty("Authorization", "Bearer " + token);
            }
            int status = connection.getResponseCode();
            InputStream stream = status >= 200 && status < 300
                    ? connection.getInputStream() : connection.getErrorStream();
            String body = readAll(stream);
            if (status != 200) {
                throw new IllegalStateException("HTTP " + status + ": " + body);
            }

            JSONArray events = new JSONArray(body);
            boolean primed = prefs.getBoolean(KEY_PRIMED, false);
            Set<String> typeFilters = splitFilters(prefs.getString(KEY_TYPES, ""));
            Set<String> keywordFilters = splitFilters(prefs.getString(KEY_KEYWORDS, ""));
            int added = 0;

            for (int i = 0; i < events.length(); i++) {
                JSONObject event = events.optJSONObject(i);
                if (event == null) {
                    continue;
                }
                String key = eventKey(event);
                if (key == null || seen.contains(key)) {
                    continue;
                }
                if (primed && matches(event, typeFilters, keywordFilters)) {
                    notifyEvent(event);
                }
                seen.add(key);
                added++;
            }

            if (!primed) {
                prefs.edit().putBoolean(KEY_PRIMED, true).apply();
            }
            if (added > 0) {
                saveSeen();
            }
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private boolean matches(JSONObject event, Set<String> typeFilters,
                            Set<String> keywordFilters) {
        List<String> types = eventTypes(event);
        if (!typeFilters.isEmpty()) {
            boolean found = false;
            for (String type : types) {
                if (typeFilters.contains(type.toLowerCase(Locale.ROOT))) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return false;
            }
        }

        if (!keywordFilters.isEmpty()) {
            String haystack = (eventText(event) + " " + TextUtils.join(" ", types) + " "
                    + event.optString("source", "")).toLowerCase(Locale.ROOT);
            boolean found = false;
            for (String keyword : keywordFilters) {
                if (haystack.contains(keyword)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return false;
            }
        }
        return true;
    }

    private void notifyEvent(JSONObject event) {
        String key = eventKey(event);
        if (key == null) {
            return;
        }
        List<String> types = eventTypes(event);
        String source = event.optString("source", "");
        String text = eventText(event);
        if (text.isEmpty()) {
            text = "收到一条新事件";
        }
        if (!source.isEmpty()) {
            text = source + " · " + text;
        }

        String title;
        if (!types.isEmpty()) {
            title = "Yi · " + TextUtils.join(", ", types);
        } else {
            title = "Yi · 新事件";
        }

        int id = NOTIFICATION_EVENT_BASE
                + (key.hashCode() & 0x7fffffff) % 1000000;
        Notification notification = newBuilder(CHANNEL_ALERTS)
                .setSmallIcon(R.drawable.ic_notification)
                .setContentTitle(title)
                .setContentText(text)
                .setStyle(new Notification.BigTextStyle().bigText(text))
                .setContentIntent(openAppIntent())
                .setAutoCancel(true)
                .setCategory(Notification.CATEGORY_MESSAGE)
                .setGroup("yi_watch")
                .setWhen(System.currentTimeMillis())
                .setShowWhen(true)
                .build();
        try {
            notifications.notify(id, notification);
        } catch (SecurityException error) {
            Log.w("YiWatch", "notification permission missing", error);
        }
    }

    private static String eventKey(JSONObject event) {
        String id = event.optString("id", "");
        if (id.isEmpty()) {
            return null;
        }
        return event.optString("source", "") + "/" + id;
    }

    private static List<String> eventTypes(JSONObject event) {
        JSONArray array = event.optJSONArray("types");
        List<String> types = new ArrayList<>();
        if (array == null) {
            return types;
        }
        for (int i = 0; i < array.length(); i++) {
            String type = array.optString(i, "");
            if (!type.isEmpty()) {
                types.add(type);
            }
        }
        return types;
    }

    private static String eventText(JSONObject event) {
        JSONObject data = event.optJSONObject("data");
        if (data == null) {
            return "";
        }
        String[] keys = {"text", "title", "subject", "summary", "body", "content", "message"};
        for (String key : keys) {
            String value = data.optString(key, "");
            if (!value.isEmpty()) {
                return truncate(value, 500);
            }
        }
        return truncate(data.toString(), 500);
    }

    private static Set<String> splitFilters(String raw) {
        Set<String> filters = new LinkedHashSet<>();
        if (raw == null || raw.trim().isEmpty()) {
            return filters;
        }
        String[] values = raw.split("[,，\\n]");
        for (String value : values) {
            String trimmed = value.trim().toLowerCase(Locale.ROOT);
            if (!trimmed.isEmpty()) {
                filters.add(trimmed);
            }
        }
        return filters;
    }

    private void loadSeen() {
        String raw = prefs.getString(KEY_SEEN, "");
        if (raw.isEmpty()) {
            return;
        }
        Collections.addAll(seen, raw.split("\\n"));
        trimSeen();
    }

    private void saveSeen() {
        trimSeen();
        StringBuilder builder = new StringBuilder();
        for (String value : seen) {
            if (builder.length() > 0) {
                builder.append('\n');
            }
            builder.append(value);
        }
        prefs.edit().putString(KEY_SEEN, builder.toString()).apply();
    }

    private void trimSeen() {
        while (seen.size() > SEEN_LIMIT) {
            java.util.Iterator<String> iterator = seen.iterator();
            iterator.next();
            iterator.remove();
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
        android.net.Uri uri = android.net.Uri.parse(value);
        if (uri.getScheme() == null || TextUtils.isEmpty(uri.getHost())) {
            return null;
        }
        if (!value.endsWith("/")) {
            value += "/";
        }
        return value;
    }

    private static String readAll(InputStream stream) throws Exception {
        if (stream == null) {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                builder.append(line).append('\n');
            }
        }
        return builder.toString();
    }

    private static String trim(String value) {
        return value == null ? "" : value.trim();
    }

    private static String truncate(String value, int limit) {
        if (value == null) {
            return "";
        }
        return value.length() <= limit ? value : value.substring(0, limit) + "…";
    }
}
