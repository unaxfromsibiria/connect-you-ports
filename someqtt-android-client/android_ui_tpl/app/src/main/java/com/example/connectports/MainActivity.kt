package com.example.connectports

import android.content.ComponentName
import android.content.Context
import android.content.Intent
import android.content.ServiceConnection
import android.content.SharedPreferences
import android.os.Bundle
import android.os.IBinder
import android.text.InputFilter
import android.util.Log
import android.view.View
import android.widget.*
import android.view.ViewGroup
import androidx.appcompat.app.AlertDialog
import androidx.appcompat.app.AppCompatActivity
import org.json.JSONArray
import org.json.JSONObject

class MainActivity : AppCompatActivity() {

    companion object {
        private const val TAG = "RustSocketApp"
        private const val PREFS_NAME = "connection_prefs"
        private const val KEY_CONNECTIONS = "connections_list"
        private const val KEY_SERVER_HOST = "server_host"
        private const val KEY_SERVER_PORT = "server_port"
        private const val KEY_AUTH_KEY = "auth_key"
        private const val KEY_TRANSPORT = "transport"
        private const val KEY_VERBOSE_LOGS = "verbose_logs"

        init {
            try {
                System.loadLibrary("socket_phone")
                Log.d(TAG, "Library loaded successfully")
            } catch (e: UnsatisfiedLinkError) {
                Log.e(TAG, "Failed to load library: ${e.message}", e)
            }
        }

        external fun startServer(
            host: String,
            port: Int,
            authKey: String,
            tcpSettings: String,
            udpSettings: String,
            verbose: Boolean,
            transport: String,
        )

        external fun stopServer()
        external fun getStat(): String
        external fun getLastError(): String
        external fun getVersion(): String
    }

    private lateinit var settingsContainer: LinearLayout
    private lateinit var btnAddRow: Button
    private lateinit var btnConnect: Button
    private lateinit var btnStats: Button
    private lateinit var serverHostInput: EditText
    private lateinit var serverPortInput: EditText
    private lateinit var authKeyInput: EditText
    private lateinit var transportSpinner: Spinner
    private lateinit var verboseLogsCheckbox: CheckBox
    private lateinit var prefs: SharedPreferences
    private lateinit var versionTextView: TextView
    private val uiVersion = "UI v1.3"
    private var lastExceptionMessage: String? = null
    private var isConnected = false
    private var rustService: RustNetworkService? = null
    private val serviceConnection = object : ServiceConnection {
        override fun onServiceConnected(name: ComponentName?, service: IBinder?) {
            val binder = service as RustNetworkService.LocalBinder
            rustService = binder.getService()
        }
        override fun onServiceDisconnected(name: ComponentName?) {
            rustService = null
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        supportActionBar?.title = "Connection Service"
        prefs = getSharedPreferences(PREFS_NAME, MODE_PRIVATE)
        val rootLayout = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            layoutParams = LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                LinearLayout.LayoutParams.MATCH_PARENT
            )
            setPadding(16, 120, 16, 26)
        }

        versionTextView = TextView(this).apply {
            text = "Loading version..."
            textSize = 12f
            setTypeface(null, android.graphics.Typeface.NORMAL)
            setTextColor(android.graphics.Color.parseColor("#808080"))
        }
        rootLayout.addView(versionTextView)

        val globalSettingsLayout = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            layoutParams = LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                LinearLayout.LayoutParams.WRAP_CONTENT
            )
            setPadding(0, 0, 0, 16)
        }

        val clientSettingsLabel = TextView(this).apply {
            text = "Client Settings:"
            textSize = 18f
            setTypeface(null, android.graphics.Typeface.BOLD)
            setPadding(0, 0, 0, 8)
        }
        globalSettingsLayout.addView(clientSettingsLabel)

        val serverRowLayout = LinearLayout(this).apply {
            orientation = LinearLayout.HORIZONTAL
            layoutParams = LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                LinearLayout.LayoutParams.WRAP_CONTENT
            )
            val childParams = LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT).apply { weight = 1f }

            serverHostInput = EditText(this@MainActivity).apply {
                hint = "Server Host"
                layoutParams = childParams
                inputType = android.text.InputType.TYPE_CLASS_TEXT or android.text.InputType.TYPE_TEXT_VARIATION_PASSWORD
                setPadding(8, 8, 8, 8)
                onFocusChangeListener = View.OnFocusChangeListener { v, hasFocus ->
                    if (hasFocus) {
                        inputType = android.text.InputType.TYPE_CLASS_TEXT or android.text.InputType.TYPE_TEXT_VARIATION_URI
                    } else {
                        inputType = android.text.InputType.TYPE_CLASS_TEXT or android.text.InputType.TYPE_TEXT_VARIATION_PASSWORD
                    }
                    setSelection(length())
                }
            }

            serverPortInput = EditText(this@MainActivity).apply {
                hint = "Server Port"
                layoutParams = childParams
                inputType = android.text.InputType.TYPE_CLASS_NUMBER
                setPadding(8, 8, 8, 8)
            }
            addView(serverHostInput)
            addView(serverPortInput)
        }

        authKeyInput = EditText(this).apply {
            hint = "Auth Key"
            layoutParams = LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                LinearLayout.LayoutParams.WRAP_CONTENT
            ).apply { topMargin = 8 }
            inputType = android.text.InputType.TYPE_CLASS_TEXT or android.text.InputType.TYPE_TEXT_VARIATION_PASSWORD
            setPadding(8, 8, 8, 8)
        }

        transportSpinner = Spinner(this).apply {
            layoutParams = LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                LinearLayout.LayoutParams.WRAP_CONTENT
            ).apply { topMargin = 8 }
            adapter = ArrayAdapter(
                this@MainActivity,
                android.R.layout.simple_spinner_item,
                listOf("mqtt", "http")
            ).also { it.setDropDownViewResource(android.R.layout.simple_spinner_dropdown_item) }
            setSelection(0)
            onItemSelectedListener = object : AdapterView.OnItemSelectedListener {
                override fun onItemSelected(parent: AdapterView<*>, view: View?, position: Int, id: Long) {
                    val selected = parent.getItemAtPosition(position)?.toString()?.lowercase() ?: "mqtt"
                    val portText = serverPortInput.text.toString().trim()
                    if (portText.isNotEmpty()) {
                        val currentPort = portText.toIntOrNull()
                        if (currentPort != null) {
                            when (selected) {
                                "mqtt" -> {
                                    if (currentPort == 8080) {
                                        serverPortInput.setText("1883")
                                    }
                                }
                                "http" -> {
                                    if (currentPort == 1883) {
                                        serverPortInput.setText("8080")
                                    }
                                }
                            }
                        }
                    }
                }
                override fun onNothingSelected(parent: AdapterView<*>) {}
            }
        }

        verboseLogsCheckbox = CheckBox(this).apply {
            text = "Verbose logs"
            layoutParams = LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                LinearLayout.LayoutParams.WRAP_CONTENT
            ).apply { topMargin = 8 }
        }

        globalSettingsLayout.addView(serverRowLayout)
        globalSettingsLayout.addView(authKeyInput)
        val transportRow = LinearLayout(this).apply {
            orientation = LinearLayout.HORIZONTAL
            layoutParams = LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                LinearLayout.LayoutParams.WRAP_CONTENT
            ).apply { topMargin = 8 }
            val spinnerParams = LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT).apply { weight = 1f }
            transportSpinner.layoutParams = spinnerParams
            addView(transportSpinner)
            addView(TextView(this@MainActivity).apply {
                text = "transport type"
                setPadding(8, 8, 8, 8)
            })
        }
        globalSettingsLayout.addView(transportRow)
        globalSettingsLayout.addView(verboseLogsCheckbox)
        rootLayout.addView(globalSettingsLayout)

        val infoText = TextView(this).apply {
            text = "Configure Connections"
            textSize = 20f
            setPadding(0, 0, 0, 16)
        }
        rootLayout.addView(infoText)

        settingsContainer = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            layoutParams = LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                0
            ).apply { weight = 1f }
        }
        rootLayout.addView(settingsContainer)

        val actionButtonsLayout = LinearLayout(this).apply {
            orientation = LinearLayout.HORIZONTAL
            layoutParams = LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                LinearLayout.LayoutParams.WRAP_CONTENT
            ).apply { topMargin = 16 }

            val mainButtonsContainer = LinearLayout(this@MainActivity).apply {
                orientation = LinearLayout.HORIZONTAL
                layoutParams = LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT).apply {
                    weight = 1f
                }
                val buttonParams = LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT).apply {
                    weight = 1f
                    marginEnd = 8
                }
                btnConnect = Button(this@MainActivity).apply {
                    text = "Connect"
                    layoutParams = buttonParams
                    setOnClickListener {
                        if (isConnected) {
                            disconnect()
                        } else {
                            connect()
                        }
                    }
                }
                btnStats = Button(this@MainActivity).apply {
                    text = "Show Statistics"
                    layoutParams = buttonParams
                    setOnClickListener {
                        showStatisticsDialog()
                    }
                }
                addView(btnConnect)
                addView(btnStats)
            }
            val quitButton = Button(this@MainActivity).apply {
                text = "Quit"
                layoutParams = LinearLayout.LayoutParams(
                    LinearLayout.LayoutParams.WRAP_CONTENT,
                    LinearLayout.LayoutParams.WRAP_CONTENT
                ).apply { marginStart = 8 }

                setOnClickListener {
                    disconnect()
                    finish()
                }
            }
            addView(mainButtonsContainer)
            addView(quitButton)
        }
        rootLayout.addView(actionButtonsLayout)
        btnAddRow = Button(this).apply {
            text = "+ Add Service"
            layoutParams = LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                LinearLayout.LayoutParams.WRAP_CONTENT
            ).apply { topMargin = 16 }
            setOnClickListener {
                addNewConnectionRow()
                saveCurrentState()
            }
        }
        rootLayout.addView(btnAddRow)
        setContentView(rootLayout)
        loadSavedConnections()
        loadGlobalSettings()
        if (settingsContainer.childCount == 0) {
            addNewConnectionRow()
        }
        serverPortInput.post {
            serverPortInput.requestFocus()
        }
        try {
            versionTextView.text = "Version: ${getVersion()} ${uiVersion}"
        } catch (e: Exception) {
            versionTextView.text = "Version info unavailable"
        }
        bindService(Intent(this, RustNetworkService::class.java), serviceConnection, Context.BIND_AUTO_CREATE)
    }

    override fun onDestroy() {
        super.onDestroy()
        try {
            unbindService(serviceConnection)
        } catch (e: Exception) {
            Log.e(TAG, "Error unbinding service", e)
        }
    }

    private fun showStatisticsDialog() {
        val statsText = try {
            getStat()
        } catch (e: Exception) {
            "Error getting statistics: ${e.message}"
        }
        val lastJniError = try {
            val err = getLastError()
            if (err.isNotBlank()) err else null
        } catch (e: Exception) {
            null
        }
        val finalMessage = buildString {
            if (lastJniError != null) {
                append("Latest processing exception: $lastJniError\n\n")
            }
            if (lastExceptionMessage != null) {
                append("Latest exception: $lastExceptionMessage\n\n")
            }
            append(statsText)
        }
        val scroll = ScrollView(this).apply { isFillViewport = true }
        val tv = TextView(this).apply {
            text = finalMessage
            setPadding(32, 16, 32, 16)
            textSize = 12f
            setTextIsSelectable(true)
        }
        scroll.addView(tv)
        AlertDialog.Builder(this)
            .setTitle("Statistics:")
            .setView(scroll)
            .setPositiveButton("OK") { dialog, _ -> dialog.dismiss() }
            .show()
    }

    private fun connect() {
        val host = serverHostInput.text.toString().trim()
        val portStr = serverPortInput.text.toString().trim()
        if (host.isEmpty()) {
            Toast.makeText(this, "Server Host is required", Toast.LENGTH_SHORT).show()
            return
        }
        val port = try {
            portStr.toInt()
        } catch (e: NumberFormatException) {
            Toast.makeText(this, "Invalid Server Port", Toast.LENGTH_SHORT).show()
            return
        }
        if (port !in 1..65535) {
            Toast.makeText(this, "Server Port out of range", Toast.LENGTH_SHORT).show()
            return
        }

        updateUIState(true)
        btnConnect.isEnabled = false
        val configs = collectAndLogConfigurations()
        val verbose = verboseLogsCheckbox.isChecked
        val authKey = authKeyInput.text.toString()
        val transport = transportSpinner.selectedItem?.toString()?.lowercase() ?: "mqtt"
        try {
            Log.d(TAG, "Starting server via Service...")
            val intent = Intent(this, RustNetworkService::class.java).apply {
                putExtra("host", host)
                putExtra("port", port)
                putExtra("auth_key", authKey)
                putExtra("tcp_settings", configs.tcp)
                putExtra("udp_settings", configs.udp)
                putExtra("verbose", verbose)
                putExtra("transport", transport)
            }

            if (android.os.Build.VERSION.SDK_INT >= android.os.Build.VERSION_CODES.O) {
                startForegroundService(intent)
            } else {
                startService(intent)
            }

            isConnected = true
            updateUIState(true)
            btnConnect.isEnabled = true
            Toast.makeText(this@MainActivity, "Server Started", Toast.LENGTH_SHORT).show()
        } catch (e: Exception) {
            Log.e(TAG, "Failed to start server service", e)
            lastExceptionMessage = e.message
            isConnected = false
            updateUIState(false)
            btnConnect.isEnabled = true
            Toast.makeText(this@MainActivity, "Error: ${e.message}", Toast.LENGTH_LONG).show()
        }
    }

    private fun disconnect() {
        try {
            rustService?.stopRustRuntime()
            Log.d(TAG, "Server stopped via Service")
        } catch (e: Exception) {
            lastExceptionMessage = e.message
            Log.e(TAG, "Failed to stop server service", e)
        }
        isConnected = false
        updateUIState(false)
    }

    private fun updateUIState(isConnected: Boolean) {
        val enabled = !isConnected
        btnConnect.text = if (isConnected) "Stop Connection" else "Connect"
        serverHostInput.isEnabled = enabled
        serverPortInput.isEnabled = enabled
        authKeyInput.isEnabled = enabled
        transportSpinner.isEnabled = enabled
        verboseLogsCheckbox.isEnabled = enabled
        btnAddRow.isEnabled = enabled

        for (i in 0 until settingsContainer.childCount) {
            val row = settingsContainer.getChildAt(i) as? LinearLayout ?: continue
            val nameInput = row.getChildAt(0) as? EditText
            nameInput?.isEnabled = enabled
            val typeSpinner = row.getChildAt(1) as? Spinner
            typeSpinner?.isEnabled = enabled
            val ipInput = row.getChildAt(2) as? EditText
            ipInput?.isEnabled = enabled
            val portInput = row.getChildAt(3) as? EditText
            portInput?.isEnabled = enabled
        }
    }

    private fun addNewConnectionRow(name: String = "", typeIndex: Int = 0, ip: String = "127.0.0.1", port: String = "") {
        val rowLayout = LinearLayout(this).apply {
            orientation = LinearLayout.HORIZONTAL
            layoutParams = LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                LinearLayout.LayoutParams.WRAP_CONTENT
            ).apply { bottomMargin = 8 }
            val childParams = LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT).apply { weight = 1f }
            // 1. Name
            val nameInput = EditText(this@MainActivity).apply {
                hint = "Name"
                layoutParams = childParams
                filters = arrayOf(InputFilter.LengthFilter(10))
                setPadding(8, 8, 8, 8)
                setText(name)
                addTextChangedListener(object : android.text.TextWatcher {
                    override fun afterTextChanged(s: android.text.Editable?) {}
                    override fun beforeTextChanged(s: CharSequence?, start: Int, count: Int, after: Int) {}
                    override fun onTextChanged(s: CharSequence?, start: Int, before: Int, count: Int) {}
                })
            }
            // 2. Type Spinner
            val typeSpinner = Spinner(this@MainActivity).apply {
                layoutParams = childParams
                adapter = ArrayAdapter(
                    this@MainActivity,
                    android.R.layout.simple_spinner_item,
                    listOf("TCP", "UDP")
                ).also { it.setDropDownViewResource(android.R.layout.simple_spinner_dropdown_item) }
                setSelection(typeIndex)
            }
            // 3. IP
            val ipInput = EditText(this@MainActivity).apply {
                hint = "IP"
                layoutParams = childParams
                inputType = android.text.InputType.TYPE_CLASS_TEXT or android.text.InputType.TYPE_TEXT_VARIATION_URI
                setPadding(8, 8, 8, 8)
                setText(ip)
            }
            // 4. Port
            val portInput = EditText(this@MainActivity).apply {
                hint = "Port"
                layoutParams = childParams
                inputType = android.text.InputType.TYPE_CLASS_NUMBER
                setPadding(8, 8, 8, 8)
                setText(port)
            }
            // 5. Delete Button (X)
            val deleteButton = ImageButton(this@MainActivity).apply {
                setImageResource(android.R.drawable.ic_menu_close_clear_cancel)
                layoutParams = LinearLayout.LayoutParams(
                    LinearLayout.LayoutParams.WRAP_CONTENT,
                    LinearLayout.LayoutParams.WRAP_CONTENT
                ).apply { marginStart = 8 }
                setOnClickListener { view ->
                    if (view.parent is ViewGroup) {
                        settingsContainer.removeView(view.parent as View)
                        saveCurrentState()
                    }
                }
            }
            addView(nameInput)
            addView(typeSpinner)
            addView(ipInput)
            addView(portInput)
            addView(deleteButton)
        }
        settingsContainer.addView(rowLayout)
    }

    private fun saveCurrentState() {
        val jsonArray = JSONArray()

        for (i in 0 until settingsContainer.childCount) {
            val row = settingsContainer.getChildAt(i) as? LinearLayout ?: continue
            val nameInput = row.getChildAt(0) as? EditText
            val typeSpinner = row.getChildAt(1) as? Spinner
            val ipInput = row.getChildAt(2) as? EditText
            val portInput = row.getChildAt(3) as? EditText
            val name = nameInput?.text.toString().trim()
            val typeIndex = typeSpinner?.selectedItemPosition ?: 0
            val ip = ipInput?.text.toString().trim()
            val port = portInput?.text.toString().trim()
            val jsonObject = JSONObject().apply {
                put("name", name)
                put("typeIndex", typeIndex)
                put("ip", ip)
                put("port", port)
            }
            jsonArray.put(jsonObject)
        }
        prefs.edit().putString(KEY_CONNECTIONS, jsonArray.toString()).apply()
        saveGlobalSettings()
    }

    private fun loadSavedConnections() {
        val jsonStr = prefs.getString(KEY_CONNECTIONS, null) ?: return
        try {
            val jsonArray = JSONArray(jsonStr)
            settingsContainer.removeAllViews()
            for (i in 0 until jsonArray.length()) {
                val obj = jsonArray.getJSONObject(i)
                val name = optString(obj, "name", "")
                val typeIndex = optInt(obj, "typeIndex", 0)
                val ip = optString(obj, "ip", "")
                val port = optString(obj, "port", "")
                addNewConnectionRow(name, typeIndex, ip, port)
            }
        } catch (e: Exception) {
            Log.e(TAG, "Error loading saved connections: ${e.message}", e)
            prefs.edit().remove(KEY_CONNECTIONS).apply()
        }
    }

    private fun optString(obj: JSONObject, key: String, default: String): String {
        return if (obj.has(key) && !obj.isNull(key)) obj.getString(key) else default
    }

    private fun optInt(obj: JSONObject, key: String, default: Int): Int {
        return if (obj.has(key) && !obj.isNull(key)) obj.getInt(key) else default
    }
    // --- Global Settings Persistence Methods ---
    private fun saveGlobalSettings() {
        val host = serverHostInput.text.toString().trim()
        val port = serverPortInput.text.toString().trim()
        val key = authKeyInput.text.toString()
        val transport = transportSpinner.selectedItem?.toString()?.lowercase() ?: "mqtt"
        val verbose = verboseLogsCheckbox.isChecked
        prefs.edit()
            .putString(KEY_SERVER_HOST, host)
            .putString(KEY_SERVER_PORT, port)
            .putString(KEY_AUTH_KEY, key)
            .putString(KEY_TRANSPORT, transport)
            .putBoolean(KEY_VERBOSE_LOGS, verbose)
            .apply()
    }

    private fun loadGlobalSettings() {
        val host = prefs.getString(KEY_SERVER_HOST, "") ?: ""
        val port = (prefs.getString(KEY_SERVER_PORT, "1883") ?: "").ifBlank { "1883" }
        val key = prefs.getString(KEY_AUTH_KEY, "") ?: ""
        val transport = prefs.getString(KEY_TRANSPORT, "mqtt") ?: "mqtt"
        val verbose = prefs.getBoolean(KEY_VERBOSE_LOGS, false)
        serverHostInput.setText(host.ifBlank { "127.0.0.1" })
        serverPortInput.setText(port)
        authKeyInput.setText(key)
        val idx = if (transport == "http") 1 else 0
        transportSpinner.setSelection(idx)
        verboseLogsCheckbox.isChecked = verbose
    }

    private data class ConfigStrings(val tcp: String, val udp: String)

    private fun collectAndLogConfigurations(): ConfigStrings {
        saveCurrentState()
        val tcpConfigs = mutableListOf<String>()
        val udpConfigs = mutableListOf<String>()
        for (i in 0 until settingsContainer.childCount) {
            val row = settingsContainer.getChildAt(i) as? LinearLayout ?: continue
            val nameInput = row.getChildAt(0) as? EditText
            val typeSpinner = row.getChildAt(1) as? Spinner
            val ipInput = row.getChildAt(2) as? EditText
            val portInput = row.getChildAt(3) as? EditText
            val name = nameInput?.text.toString().trim()
            val type = (typeSpinner?.selectedItem as? String).orEmpty()
            val ip = ipInput?.text.toString().trim()
            val portStr = portInput?.text.toString().trim()
            if (name.isEmpty() || ip.isEmpty()) {
                Log.w(TAG, "Skipping row $i due to empty Name or IP")
                continue
            }
            val port = try {
                portStr.toInt()
            } catch (e: NumberFormatException) {
                Log.e(TAG, "Invalid port format in row $i: $portStr")
                -1
            }
            if (port !in 1..65535) {
                Log.e(TAG, "Port out of range [1-65535] in row $i: $port")
                continue
            }
            val configString = "$name:$ip:$port"
            when (type.uppercase()) {
                "TCP" -> tcpConfigs.add(configString)
                "UDP" -> udpConfigs.add(configString)
                else -> Log.w(TAG, "Unknown type '$type' in row $i")
            }
        }
        val finalTcpString = tcpConfigs.joinToString(";")
        val finalUdpString = udpConfigs.joinToString(";")
        Log.d(TAG, "TCP services: $finalTcpString")
        Log.d(TAG, "UDP services: $finalUdpString")
        return ConfigStrings(finalTcpString, finalUdpString)
    }
}
