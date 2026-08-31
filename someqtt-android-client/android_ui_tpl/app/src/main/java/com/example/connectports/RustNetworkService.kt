package com.example.connectports

import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.Service
import android.content.Context
import android.content.Intent
import android.os.Binder
import android.os.IBinder
import android.util.Log
import androidx.core.app.NotificationCompat

class RustNetworkService : Service() {

    companion object {
        private const val TAG = "RustNetworkSvc"
        private const val CHANNEL_ID = "rust_network_channel"
        private const val NOTIFICATION_ID = 1001
    }
    private val binder = LocalBinder()
    private var isRunning = false

    inner class LocalBinder : Binder() {
        fun getService(): RustNetworkService = this@RustNetworkService
    }

    override fun onBind(intent: Intent?): IBinder = binder
    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        val host = intent?.getStringExtra("host") ?: ""
        val port = intent?.getIntExtra("port", 0) ?: 0
        val authKey = intent?.getStringExtra("auth_key") ?: ""
        val tcpSettings = intent?.getStringExtra("tcp_settings") ?: ""
        val udpSettings = intent?.getStringExtra("udp_settings") ?: ""
        val verbose = intent?.getBooleanExtra("verbose", false) ?: false
        if (!isRunning && host.isNotEmpty() && port > 0) {
            startRustRuntime(host, port, authKey, tcpSettings, udpSettings, verbose)
        }
        return START_STICKY
    }

    private fun startRustRuntime(
        host: String,
        port: Int,
        authKey: String,
        tcpSettings: String,
        udpSettings: String,
        verbose: Boolean
    ) {
        if (isRunning) return
        isRunning = true
        createNotificationChannel()
        val notification = NotificationCompat.Builder(this, CHANNEL_ID)
            .setContentTitle("Connection Service Active")
            .setContentText("Connected to $host:$port")
            .setSmallIcon(android.R.drawable.ic_dialog_info)
            .setOngoing(true) 
            .build()

        try {
            startForeground(NOTIFICATION_ID, notification)
            MainActivity.startServer(host, port, authKey, tcpSettings, udpSettings, verbose)
            Log.d(TAG, "Rust server started in service")
        } catch (e: Exception) {
            Log.e(TAG, "Failed to start Rust server", e)
            isRunning = false
            stopForeground(Service.STOP_FOREGROUND_REMOVE)
            stopSelf()
        }
    }

    fun stopRustRuntime() {
        if (!isRunning) return
        try {
            MainActivity.stopServer()
            Log.d(TAG, "Rust server stopped")
        } catch (e: Exception) {
            Log.e(TAG, "Failed to stop Rust server", e)
        } finally {
            isRunning = false
            stopForeground(Service.STOP_FOREGROUND_REMOVE)
            stopSelf()
        }
    }

    override fun onDestroy() {
        if (isRunning) {
            stopRustRuntime()
        }
        super.onDestroy()
    }

    private fun createNotificationChannel() {
        val channel = NotificationChannel(
            CHANNEL_ID,
            "Network Service",
            android.app.NotificationManager.IMPORTANCE_LOW
        ).apply {
            description = "Keeps the connection active in background"
        }
        val manager = getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
        manager.createNotificationChannel(channel)
    }
}
