# select Arduino Pro or Pro Mini - or something else, depending on your hardware.


#include <limits.h>

const int BUZZER_PIN = 2; // Your buzzer pin
const long BAUD_RATE = 115200; // Serial communication baud rate
const byte BUFFER_SIZE = 32;

char inputBuffer[BUFFER_SIZE];
byte bufferIndex = 0;
bool inputOverflow = false;
bool inputInvalid = false;

bool parseCommand(const char *input, unsigned long &numBeeps,
                  unsigned long &highDelay, unsigned long &pauseDelay) {
  unsigned long values[3];

  for (byte field = 0; field < 3; field++) {
    if (*input < '0' || *input > '9') {
      return false;
    }

    unsigned long value = 0;
    while (*input >= '0' && *input <= '9') {
      const byte digit = *input - '0';
      if (value > (ULONG_MAX - digit) / 10) {
        return false;
      }
      value = value * 10 + digit;
      input++;
    }
    values[field] = value;

    if (field < 2) {
      if (*input != ',') {
        return false;
      }
      input++;
    } else if (*input != '\0') {
      return false;
    }
  }

  if (values[0] == 0 || values[1] == 0) {
    return false;
  }

  numBeeps = values[0];
  highDelay = values[1];
  pauseDelay = values[2];
  return true;
}

void setup() {
  Serial.begin(BAUD_RATE);
  pinMode(BUZZER_PIN, OUTPUT);
  digitalWrite(BUZZER_PIN, LOW);
}

void loop() {
  while (Serial.available() > 0) {
    char incomingChar = (char)Serial.read();

    if (incomingChar == '\n' || incomingChar == '\r') {
      if (inputOverflow) {
        Serial.println("ERROR: too long.");
      } else if (inputInvalid) {
        Serial.println("INFO: Command contains a null character; discarded.");
      } else if (bufferIndex > 0) {
        inputBuffer[bufferIndex] = '\0';

        unsigned long numBeeps = 0;
        unsigned long highDelay = 0;
        unsigned long pauseDelay = 0;

        if (parseCommand(inputBuffer, numBeeps, highDelay, pauseDelay)) {
          Serial.print("ACK");

          for (unsigned long i = 0; i < numBeeps; i++) {
            digitalWrite(BUZZER_PIN, HIGH);
            delay(highDelay);
            digitalWrite(BUZZER_PIN, LOW);

            if (i + 1 < numBeeps) {
              delay(pauseDelay);
            }
          }
          Serial.println("DONE");
        } else {
          Serial.println("ERROR: Use three valid decimal values: numBeeps,highDelay,pauseDelay.");
        }
      }
      bufferIndex = 0;
      inputOverflow = false;
      inputInvalid = false;
    } else if (!inputOverflow) {
      if (incomingChar == '\0') {
        inputInvalid = true;
      } else if (bufferIndex < BUFFER_SIZE - 1) {
        inputBuffer[bufferIndex++] = incomingChar;
      } else {
        inputOverflow = true;
      }
    }
  }
}

