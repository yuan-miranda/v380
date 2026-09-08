#include <WiFi.h>
#include <HTTPClient.h>

const char* ssid = "Converge_2.4GHz_BtM2";
const char* password = "Xd4AjFnJ";

// Replace with your computer's local IP running Python Flask
const char* serverIp = "192.168.100.58"; 

const int buttonPins[] = {15, 16, 17};
const int numButtons = 3;

int buttonState[numButtons];
int lastButtonState[numButtons];
unsigned long clickCount[numButtons];
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
    clickCount[i] = 0;
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
          clickCount[i]++;
          int buttonNumber = i + 1; // Maps pin index to Button 1, 2, or 3
          
          Serial.print("Button ");
          Serial.print(buttonNumber);
          Serial.println(" clicked! Sending SMS request...");

          if (WiFi.status() == WL_CONNECTED) {
            HTTPClient http;
            
            // Construct URL dynamically like http://192.168.100.X:5000/send-sms?button=1
            String url = String("http://") + serverIp + ":5000/send-sms?button=" + String(buttonNumber);
            
            http.begin(url);
            int httpResponseCode = http.GET();
            
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