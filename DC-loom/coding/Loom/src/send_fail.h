#ifndef send_fail_H
#define send_fail_H

// #include <Arduino.h>
#include <SD.h>
//#include <employee.h>
#include <SPIFFS.h>
long file_count = 0;
#define SD_CS 4 // Chip select pin
SPIClass spiSD(VSPI);
int jsons(String str);
void failed(String str)
{
    char name[30];
    spiSD.begin(14, 12, 13, SD_CS);
    if (!SD.begin(SD_CS, spiSD))
    {
        Serial.println("SD Card Mount Failed");
        return;
    }

    uint8_t cardType = SD.cardType();
    if (cardType == CARD_NONE)
    {
        Serial.println("No SD card attached");
        return;
    }
    struct tm timeinfo;
    getLocalTime(&timeinfo);
    strftime(name, sizeof(name), "%Y%m%dT%H%M%S000Z", &timeinfo);
    String file_name = "/" + String(name) + ".json";

    File file = SD.open(file_name, FILE_WRITE);
    file.print(str);
    file.close();

    Serial.println("File stored");
    SD.end();
}

void Send()
{

    if (!SD.begin(SD_CS))
    {
        Serial.println("SD Card Mount Failed");
        return;
    }

    File root = SD.open("/");
    if (!root || !root.isDirectory())
    {
        Serial.println("Failed to open SD root");
        return;
    }
    File file = root.openNextFile();
    while (file)
    {
        String file_name = "/"+String(file.name());
        String data = file.readString();
        file.close();
        if (jsons(data) != 200)
        {
            failed(data);
            break;
        }
        Serial.println(file_name);
        if (SD.exists(file_name))
        {
            SD.remove(file_name);
            Serial.println("File_Removed");
        }
        file = root.openNextFile();
        vTaskDelay(1000 / portTICK_PERIOD_MS);
    }
    SD.end();
}

// void remove()
// {
//     File root = SPIFFS.open("/");
//     File file = root.openNextFile();
//     while (file)
//     {
//         String file_name = "/" + String(file.name());
//         Serial.println(file_name);
//         file.close();
//         size_t fsize = file.size();
//         if (fsize == 0)
//         {
//             if (SPIFFS.exists(file_name))
//             {
//                 if (SPIFFS.remove(file_name))
//                 Serial.println("Removed Succesfully");
//             }
//         }
//         file = root.openNextFile();
//     }
// }
#endif