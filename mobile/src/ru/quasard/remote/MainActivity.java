package ru.quasard.remote;

import android.app.Activity;
import android.bluetooth.BluetoothAdapter;
import android.bluetooth.BluetoothDevice;
import android.bluetooth.BluetoothSocket;
import android.content.SharedPreferences;
import android.os.Bundle;
import android.webkit.JavascriptInterface;
import android.webkit.WebSettings;
import android.webkit.WebView;
import android.webkit.WebViewClient;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Set;
import java.util.UUID;

/**
 * Пульт колонки: веб-интерфейс в WebView, команды — на lite по HTTP
 * (/_mgmt/... с токеном), настройка чужого Wi-Fi — по Bluetooth (SPP).
 */
public class MainActivity extends Activity {
    private WebView web;
    private SharedPreferences prefs;

    private static final UUID SPP_UUID =
            UUID.fromString("00001101-0000-1000-8000-00805F9B34FB");

    @Override
    protected void onCreate(Bundle b) {
        super.onCreate(b);
        prefs = getSharedPreferences("cfg", MODE_PRIVATE);
        web = new WebView(this);
        WebSettings s = web.getSettings();
        s.setJavaScriptEnabled(true);
        s.setDomStorageEnabled(true);
        web.addJavascriptInterface(new Bridge(), "Android");
        web.setWebViewClient(new WebViewClient());
        web.loadUrl("file:///android_asset/ui.html");
        setContentView(web);
    }

    public class Bridge {
        // ---------------------------------------------------------- настройки
        @JavascriptInterface
        public String settings() {
            String ip = prefs.getString("ip", "");
            String tok = prefs.getString("token", "");
            return "{\"ip\":\"" + esc(ip) + "\",\"token\":\"" + esc(tok) + "\"}";
        }

        @JavascriptInterface
        public void save(String ip, String token) {
            prefs.edit().putString("ip", ip.trim())
                    .putString("token", token.trim()).apply();
        }

        // ---------------------------------------------------------- HTTP к колонке
        /** Синхронный HTTP к колонке. Вызывается из JS-потока (не UI). */
        @JavascriptInterface
        public String api(String method, String path, String body) {
            String ip = prefs.getString("ip", "");
            String tok = prefs.getString("token", "");
            if (ip.isEmpty()) {
                return "{\"error\":\"не задан адрес колонки (вкладка Настройки)\"}";
            }
            HttpURLConnection c = null;
            try {
                c = (HttpURLConnection) new URL("http://" + ip + path).openConnection();
                c.setRequestMethod(method);
                c.setConnectTimeout(6000);
                c.setReadTimeout(40000);   // сканирование Wi-Fi ~5 с
                c.setRequestProperty("X-Token", tok);
                if (body != null && !body.isEmpty()) {
                    c.setDoOutput(true);
                    c.setRequestProperty("Content-Type", "application/json");
                    OutputStream os = c.getOutputStream();
                    os.write(body.getBytes("UTF-8"));
                    os.close();
                }
                int code = c.getResponseCode();
                InputStream is = (code >= 400) ? c.getErrorStream() : c.getInputStream();
                if (is == null) {
                    return "{\"error\":\"HTTP " + code + "\"}";
                }
                BufferedReader r = new BufferedReader(new InputStreamReader(is, "UTF-8"));
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = r.readLine()) != null) sb.append(line);
                r.close();
                return sb.toString();
            } catch (Exception e) {
                String msg = e.getClass().getSimpleName();
                if (e.getMessage() != null) msg += ": " + e.getMessage();
                return "{\"error\":\"" + esc(msg) + "\"}";
            } finally {
                if (c != null) c.disconnect();
            }
        }

        // ---------------------------------------------------------- Bluetooth
        @JavascriptInterface
        public String btDevices() {
            try {
                BluetoothAdapter a = BluetoothAdapter.getDefaultAdapter();
                if (a == null) return "{\"error\":\"нет Bluetooth на телефоне\"}";
                if (!a.isEnabled()) return "{\"error\":\"Bluetooth выключен — включите в настройках Android\"}";
                Set<BluetoothDevice> bonded = a.getBondedDevices();
                StringBuilder sb = new StringBuilder("[");
                boolean first = true;
                if (bonded != null) {
                    for (BluetoothDevice d : bonded) {
                        if (!first) sb.append(",");
                        first = false;
                        String name = d.getName() == null ? "" : d.getName();
                        sb.append("{\"name\":\"").append(esc(name))
                                .append("\",\"mac\":\"").append(d.getAddress()).append("\"}");
                    }
                }
                return "{\"devices\":" + sb + "}";
            } catch (Exception e) {
                return "{\"error\":\"" + esc(String.valueOf(e)) + "\"}";
            }
        }

        /** Передать колонке SSID и пароль по Bluetooth. Возвращает ответ
         * колонки как есть: {"ok":true,"ip":"..."} или {"error":"..."}.
         * Сетевой ввод-вывод — в отдельном потоке (JS-поток ждёт до 45 с). */
        @JavascriptInterface
        public String btProvision(final String mac, final String ssid,
                                  final String password) {
            final String[] result = {null};
            Thread t = new Thread(new Runnable() {
                public void run() {
                    try {
                        result[0] = doProvision(mac, ssid, password);
                    } catch (Exception e) {
                        result[0] = "{\"error\":\"" + esc(String.valueOf(e)) + "\"}";
                    }
                }
            });
            t.start();
            try {
                t.join(45000);
            } catch (InterruptedException ignored) {
            }
            return result[0] != null ? result[0]
                    : "{\"error\":\"колонка не ответила по Bluetooth за 45 с\"}";
        }

        private String doProvision(String mac, String ssid, String password)
                throws Exception {
            BluetoothAdapter a = BluetoothAdapter.getDefaultAdapter();
            if (a == null) return "{\"error\":\"нет Bluetooth\"}";
            a.cancelDiscovery();
            BluetoothDevice dev = a.getRemoteDevice(mac);
            if (dev.getBondState() != BluetoothDevice.BOND_BONDED) {
                dev.createBond();
                long end = System.currentTimeMillis() + 20000;
                while (dev.getBondState() != BluetoothDevice.BOND_BONDED) {
                    if (System.currentTimeMillis() > end) {
                        return "{\"error\":\"не удалось сопрячься — подтвердите сопряжение на телефоне\"}";
                    }
                    Thread.sleep(300);
                }
            }
            BluetoothSocket sock = null;
            try {
                sock = dev.createRfcommSocketToServiceRecord(SPP_UUID);
                sock.connect();
            } catch (Exception e) {
                // SDP-записи может не быть — открываем канал 1 напрямую
                if (sock != null) {
                    try { sock.close(); } catch (Exception ignored) {}
                }
                java.lang.reflect.Method m =
                        dev.getClass().getMethod("createRfcommSocket", int.class);
                sock = (BluetoothSocket) m.invoke(dev, 1);
                sock.connect();
            }
            try {
                String req = "{\"ssid\":\"" + esc(ssid) + "\",\"password\":\""
                        + esc(password) + "\"}\n";
                OutputStream os = sock.getOutputStream();
                os.write(req.getBytes("UTF-8"));
                os.flush();
                StringBuilder sb = new StringBuilder();
                InputStream is = sock.getInputStream();
                int b;
                while ((b = is.read()) != -1) {
                    if (b == '\n') break;
                    sb.append((char) b);
                    if (sb.length() > 4096) break;
                }
                String out = sb.toString();
                return out.isEmpty() ? "{\"error\":\"колонка не ответила\"}" : out;
            } finally {
                try { sock.close(); } catch (Exception ignored) {}
            }
        }

        private String esc(String s) {
            if (s == null) return "";
            return s.replace("\\", "\\\\").replace("\"", "\\\"")
                    .replace("\n", " ").replace("\r", " ");
        }
    }
}
