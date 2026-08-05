package com.example.connectports

import android.content.SharedPreferences
import android.os.Bundle
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
            verbose: Boolean
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
    // Global settings fields
    private lateinit var serverHostInput: EditText
    private lateinit var serverPortInput: EditText
    private lateinit var authKeyInput: EditText
    private lateinit var verboseLogsCheckbox: CheckBox
    private lateinit var prefs: SharedPreferences
    private lateinit var versionTextView: TextView
    private var lastExceptionMessage: String? = null
    // Track connection state
    private var isConnected = false

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
        // --- Global Settings Section (Host, Port, Key) ---
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
        // Row for Host and Port
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
        // Field for Auth Key (Password style)
        authKeyInput = EditText(this).apply {
            hint = "Auth Key"
            layoutParams = LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                LinearLayout.LayoutParams.WRAP_CONTENT
            ).apply { topMargin = 8 }
            inputType = android.text.InputType.TYPE_CLASS_TEXT or android.text.InputType.TYPE_TEXT_VARIATION_PASSWORD
            setPadding(8, 8, 8, 8)
        }
        // Verbose Logs Checkbox
        verboseLogsCheckbox = CheckBox(this).apply {
            text = "Verbose logs"
            layoutParams = LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                LinearLayout.LayoutParams.WRAP_CONTENT
            ).apply { topMargin = 8 }
        }
        globalSettingsLayout.addView(serverRowLayout)
        globalSettingsLayout.addView(authKeyInput)
        globalSettingsLayout.addView(verboseLogsCheckbox)
        rootLayout.addView(globalSettingsLayout)
        // --- Existing Connections Section ---
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
        // --- Action Buttons Container (Connect + Stats side-by-side, Quit on right) ---
        val actionButtonsLayout = LinearLayout(this).apply {
            orientation = LinearLayout.HORIZONTAL
            layoutParams = LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT,
                LinearLayout.LayoutParams.WRAP_CONTENT
            ).apply { topMargin = 16 }
            // Layout for Connect and Stats buttons to share space evenly on the left/center
            val mainButtonsContainer = LinearLayout(this@MainActivity).apply {
                orientation = LinearLayout.HORIZONTAL
                layoutParams = LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT).apply {
                    weight = 1f // Takes up remaining space except for Quit button
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
            // Quit Button aligned to the right
            val quitButton = Button(this@MainActivity).apply {
                text = "Quit"
                layoutParams = LinearLayout.LayoutParams(
                    LinearLayout.LayoutParams.WRAP_CONTENT,
                    LinearLayout.LayoutParams.WRAP_CONTENT
                ).apply { marginStart = 8 }

                setOnClickListener {
                    disconnect()
                    finish() // Closes the activity/app
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
            versionTextView.text = "Version: ${getVersion()}"
        } catch (e: Exception) {
            versionTextView.text = "Version info unavailable"
        }
    }
    // --- New Function for Statistics Dialog ---
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
        AlertDialog.Builder(this)
            .setTitle("Statistics:")
            .setMessage(finalMessage) 
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

        try {
            Log.d(TAG, "Starting server...")
            startServer(host, port, authKey, configs.tcp, configs.udp, verbose)
            lastExceptionMessage = null
            runOnUiThread {
                isConnected = true
                updateUIState(true)
                btnConnect.isEnabled = true
                Toast.makeText(this@MainActivity, "Server Started", Toast.LENGTH_SHORT).show()
            }
        } catch (e: Exception) {
            Log.e(TAG, "Failed to start server in background", e)
            lastExceptionMessage = e.message
            runOnUiThread {
                isConnected = false
                updateUIState(false)
                btnConnect.isEnabled = true
                Toast.makeText(this@MainActivity, "Error: ${e.message}", Toast.LENGTH_LONG).show()
            }
        }
    }

    private fun disconnect() {
        try {
            stopServer()
            Log.d(TAG, "Server stopped")
        } catch (e: Exception) {
            lastExceptionMessage = e.message
            Log.e(TAG, "Failed to stop server", e)
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

    private fun addNewConnectionRow(name: String = "", typeIndex: Int = 0, ip: String = "", port: String = "") {
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
        val verbose = verboseLogsCheckbox.isChecked
        prefs.edit()
            .putString(KEY_SERVER_HOST, host)
            .putString(KEY_SERVER_PORT, port)
            .putString(KEY_AUTH_KEY, key)
            .putBoolean(KEY_VERBOSE_LOGS, verbose)
            .apply()
    }

    private fun loadGlobalSettings() {
        val host = prefs.getString(KEY_SERVER_HOST, "") ?: ""
        val port = prefs.getString(KEY_SERVER_PORT, "") ?: ""
        val key = prefs.getString(KEY_AUTH_KEY, "") ?: ""
        val verbose = prefs.getBoolean(KEY_VERBOSE_LOGS, false)
        serverHostInput.setText(host)
        serverPortInput.setText(port)
        authKeyInput.setText(key)
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
