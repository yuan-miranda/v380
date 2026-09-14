#include <WiFi.h>
#include <HTTPClient.h>

const char* ssid = "Converge_2.4GHz_BtM2";
const char* password = "Xd4AjFnJ";

// VPS public IP; button events are queued there for the phone to poll.
const char* serverIp = "178.128.82.49";
const char* eventToken = "qqqq";

const int buttonPins[] = {15, 16, 17};
const int numButtons = 3;

int buttonState[numButtons];
int lastButtonState[numButtons];
unsigned long lastDebounceTime[numButtons];
unsigned long debounceDelay = 50;

void setup() {
  Serial.begin(115200);

  WiFi.begin(ssid, password);
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
  }
  Serial.println("\nConnected to WiFi!");

  for (int i = 0; i < numButtons; i++) {
    pinMode(buttonPins[i], INPUT_PULLUP);
    buttonState[i] = digitalRead(buttonPins[i]);
    lastButtonState[i] = HIGH;
    lastDebounceTime[i] = 0;
  }
}

void loop() {
  for (int i = 0; i < numButtons; i++) {
    int reading = digitalRead(buttonPins[i]);

    if (reading != lastButtonState[i]) {
      lastDebounceTime[i] = millis();
    }

    if ((millis() - lastDebounceTime[i]) > debounceDelay) {
      if (reading != buttonState[i]) {
        if (buttonState[i] == LOW && reading == HIGH) {
          int buttonNumber = i + 1; // Maps pin index to Button 1, 2, or 3
          
          Serial.print("Button ");
          Serial.print(buttonNumber);
          Serial.println(" clicked! Sending event to VPS...");

          if (WiFi.status() == WL_CONNECTED) {
            HTTPClient http;
            
            String url = String("http://") + serverIp + ":5000/events";
            
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