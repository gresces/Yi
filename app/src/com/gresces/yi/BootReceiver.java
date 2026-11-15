package com.gresces.yi;

import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;
import android.os.Build;
import android.util.Log;

/** Restart the optional foreground watcher after a reboot. */
public final class BootReceiver extends BroadcastReceiver {
    @Override
    public void onReceive(Context context, Intent intent) {
        if (intent == null || !Intent.ACTION_BOOT_COMPLETED.equals(intent.getAction())) {
            return;
        }
        SharedPreferences prefs = context.getSharedPreferences(
                YiWatchService.PREFS, Context.MODE_PRIVATE);
        if (!prefs.getBoolean(YiWatchService.KEY_ENABLED, false)) {
            return;
        }
        try {
            Intent service = new Intent(context, YiWatchService.class);
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                context.startForegroundService(service);
            } else {
                context.startService(service);
            }
        } catch (RuntimeException error) {
            Log.w("YiBoot", "cannot start watcher after boot", error);
        }
    }
}
