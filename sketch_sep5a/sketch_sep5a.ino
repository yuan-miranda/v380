#include <WiFi.h>
#include <HTTPClient.h>

const char* ssid = "Converge_2.4GHz_BtM2";
const char* password = "Xd4AjFnJ";

const char* serverHost = "alerto.ddns.net";
const char* eventToken = "qqqq";

// 18 pin

const int buttonPins[] = {15, 16, 17};
const int numButtons = 3;

int buttonState[numButtons];
int lastButtonState[numButtons];
unsigned long lastDebounceTime[numButtons];
unsigned long debounceDelay = 50;

// ---------------- Relay ----------------
const int RELAY_PIN = 18;
const bool RELAY_ACTIVE_LOW = true;   // set true if your relay module triggers on LOW
const int RELAY_POLL_WAIT_SECONDS = 20; // long-poll: server holds the request until a command arrives

portMUX_TYPE relayMux = portMUX_INITIALIZER_UNLOCKED;

bool relayState = false;               // logical state: true = ON
bool relayTimerActive = false;
unsigned long relayRevertAt = 0;

// Beep pattern state (played locally by the ESP32, one step at a time)
bool patternActive = false;
bool patternPhaseOn = false;           // true = currently in a beep, false = in the pause
int patternBeepsLeft = 0;
unsigned long patternOnMs = 0;
unsigned long patternOffMs = 0;
unsigned long patternPhaseEnd = 0;

void writeRelayPin(bool on) {
  digitalWrite(RELAY_PIN, (on != RELAY_ACTIVE_LOW) ? HIGH : LOW);
}

// power   : true = relay ON, false = relay OFF
// seconds : how long to hold that state, then flip back automatically.
//           0 = hold until the next command.
// Non-blocking, so buttons keep working while the timer runs.
void relayControl(bool power, int seconds) {
  if (seconds < 0) seconds = 0;

  portENTER_CRITICAL(&relayMux);
  patternActive = false;
  relayState = power;
  writeRelayPin(power);
  relayTimerActive = (seconds > 0);
  relayRevertAt = millis() + (unsigned long)seconds * 1000UL;
  portEXIT_CRITICAL(&relayMux);

  Serial.print("[RELAY] ");
  Serial.print(power ? "ON" : "OFF");
  if (seconds > 0) {
    Serial.print(" for ");
    Serial.print(seconds);
    Serial.println("s, then it will flip back");
  } else {
    Serial.println(" (holding until next command)");
  }
}

// Rings the relay 'beeps' times: ON for onSeconds, OFF for offSeconds, repeat.
// Example: relayPattern(3, 1.0, 1.0) -> BEEP(1s) pause(1s) BEEP(1s) pause(1s) BEEP(1s)
// Non-blocking: loop() -> relayUpdate() steps through it, so buttons keep working.
void relayPattern(int beeps, float onSeconds, float offSeconds) {
  if (beeps < 1) return;
  if (onSeconds < 0.05f) onSeconds = 0.05f;
  if (offSeconds < 0.05f) offSeconds = 0.05f;
  unsigned long onMs = (unsigned long)(onSeconds * 1000.0f);
  unsigned long offMs = (unsigned long)(offSeconds * 1000.0f);

  portENTER_CRITICAL(&relayMux);
  relayTimerActive = false;
  patternActive = true;
  patternBeepsLeft = beeps;
  patternOnMs = onMs;
  patternOffMs = offMs;
  patternPhaseOn = true;
  patternPhaseEnd = millis() + onMs;
  relayState = true;
  writeRelayPin(true);
  portEXIT_CRITICAL(&relayMux);

  Serial.print("[RELAY] Pattern started: ");
  Serial.print(beeps);
  Serial.print(" beeps, ");
  Serial.print(onSeconds);
  Serial.print("s on / ");
  Serial.print(offSeconds);
  Serial.println("s off");
}

bool relayPatternRunning() {
  portENTER_CRITICAL(&relayMux);
  bool running = patternActive;
  portEXIT_CRITICAL(&relayMux);
  return running;
}

// Call from loop(): flips the relay back when the timer runs out.
void relayUpdate() {
  bool reverted = false;
  bool newState = false;
  bool patternFinished = false;

  portENTER_CRITICAL(&relayMux);
  if (relayTimerActive && (long)(millis() - relayRevertAt) >= 0) {
    relayTimerActive = false;
    relayState = !relayState;
    writeRelayPin(relayState);
    reverted = true;
    newState = relayState;
  }

  if (patternActive && (long)(millis() - patternPhaseEnd) >= 0) {
    if (patternPhaseOn) {
      // beep just ended -> relay OFF
      relayState = false;
      writeRelayPin(false);
      patternBeepsLeft--;
      if (patternBeepsLeft <= 0) {
        patternActive = false;
        patternFinished = true;
      } else {
        patternPhaseOn = false;
        patternPhaseEnd += patternOffMs;
      }
    } else {
      // pause just ended -> next beep, relay ON
      relayState = true;
      writeRelayPin(true);
      patternPhaseOn = true;
      patternPhaseEnd += patternOnMs;
    }
  }
  portEXIT_CRITICAL(&relayMux);

  if (reverted) {
    Serial.print("[RELAY] Timer done, relay is now ");
    Serial.println(newState ? "ON" : "OFF");
  }
  if (patternFinished) {
    Serial.println("[RELAY] Pattern finished, relay is OFF");
  }
}

// Reads the value after "key": in a small JSON string (no JSON library needed).
// Returns the index just past the colon, or -1 if the key isn't there.
int jsonValueStart(const String& json, const char* key) {
  int k = json.indexOf(String("\"") + key + "\"");
  if (k < 0) return -1;
  int colon = json.indexOf(':', k);
  if (colon < 0) return -1;
  int i = colon + 1;
  while (i < (int)json.length() && json[i] == ' ') i++;
  return i;
}

void handleRelayPayload(const String& payload) {
  // Beep pattern: {"beeps":3,"on_seconds":1.0,"off_seconds":1.0,...}
  int b = jsonValueStart(payload, "beeps");
  if (b >= 0) {
    int beeps = payload.substring(b).toInt();
    int on = jsonValueStart(payload, "on_seconds");
    int off = jsonValueStart(payload, "off_seconds");
    float onSeconds = (on >= 0) ? payload.substring(on).toFloat() : 1.0f;
    float offSeconds = (off >= 0) ? payload.substring(off).toFloat() : 1.0f;
    relayPattern(beeps, onSeconds, offSeconds);
    return;
  }

  // Simple on/off: {"power":true,"duration":10,...}
  int p = jsonValueStart(payload, "power");
  if (p < 0) return;  // {"command":null} -> nothing to do

  bool power = payload.startsWith("true", p);
  int d = jsonValueStart(payload, "duration");
  int seconds = (d >= 0) ? payload.substring(d).toInt() : 0;

  relayControl(power, seconds);
}

// Listens to the server like main.py does: long-poll /relay/next, the server holds the
// request open and answers the moment a relay command is queued. Runs on its own task
// so waiting never blocks button reading.
void relayListenerTask(void* param) {
  int failures = 0;
  for (;;) {
    if (WiFi.status() != WL_CONNECTED) {
      vTaskDelay(pdMS_TO_TICKS(1000));
      continue;
    }

    HTTPClient http;
    http.begin(String("http://") + serverHost + "/relay/next?wait=" + RELAY_POLL_WAIT_SECONDS);
    http.setTimeout((RELAY_POLL_WAIT_SECONDS + 10) * 1000);
    http.addHeader("Authorization", String("Bearer ") + eventToken);

    int code = http.GET();
    if (code == 200) {
      failures = 0;
      String payload = http.getString();
      http.end();
      handleRelayPayload(payload);  // {"command":null} on timeout -> ignored

      // Let a beep pattern finish before asking for the next command, so several
      // queued patterns play one after another instead of cutting each other off.
      while (relayPatternRunning()) {
        vTaskDelay(pdMS_TO_TICKS(50));
      }
    } else {
      failures++;
      Serial.print("[RELAY] Listen error (attempt ");
      Serial.print(failures);
      Serial.print("), HTTP ");
      Serial.println(code);
      http.end();
      vTaskDelay(pdMS_TO_TICKS(failures >= 5 ? 2000 : 200));
    }
  }
}

void setup() {
  Serial.begin(115200);

  WiFi.begin(ssid, password);
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
  }
  Serial.println("\nConnected to WiFi!");

  pinMode(RELAY_PIN, OUTPUT);
  writeRelayPin(false);  // start with the relay OFF

  for (int i = 0; i < numButtons; i++) {
    pinMode(buttonPins[i], INPUT_PULLUP);
    buttonState[i] = digitalRead(buttonPins[i]);
    lastButtonState[i] = HIGH;
    lastDebounceTime[i] = 0;
  }

  xTaskCreatePinnedToCore(relayListenerTask, "relayListener", 8192, NULL, 1, NULL, 0);
  Serial.println("Listening to server for relay commands...");
}

void loop() {
  relayUpdate();  // flips the relay back when its timer ends

  for (int i = 0; i < numButtons; i++) {
    int reading = digitalRead(buttonPins[i]);

    if (reading != lastButtonState[i]) {
      lastDebounceTime[i] = millis();
    }

    if ((millis() - lastDebounceTime[i]) > debounceDelay) {
      if (reading != buttonState[i]) {
        if (buttonState[i] == LOW && reading == HIGH) {
          int buttonNumber = i + 1;
          
          Serial.print("Button ");
          Serial.print(buttonNumber);
          Serial.println(" clicked! Sending event to VPS...");

          if (WiFi.status() == WL_CONNECTED) {
            HTTPClient http;
            
            String url = String("http://") + serverHost + "/events";
            
            http.begin(url);
            http.addHeader("Authorization", String("Bearer ") + eventToken);
            http.addHeader("Content-Type", "application/json");
            String body = String("{\"button\":\"") + buttonNumber + "\"}";
            int httpResponseCode = http.POST(body);
            
            if (httpResponseCode > 0) {
              Serial.print("Server Response Code: ");
              Serial.println(httpResponseCode);
            } else {
              Serial.print("HTTP Error: ");
              Serial.println(httpResponseCode);
            }
            http.end();
          }
        }
        buttonState[i] = reading;
      }
    }
    lastButtonState[i] = reading;
  }
}