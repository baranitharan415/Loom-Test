#include <WiFi.h>
#include <EEPROM.h>
#include <Arduino.h>
#include <ArduinoJson.h>
#include <PubSubClient.h>
#include <HTTPClient.h>
#include <SPIFFS.h>
#include <send_fail.h>
#include <stdlib.h>
#include "employee.h"


HardwareSerial NewSerial(2);

int led = 2;
int pubmillis =0;
HTTPClient http;
String input;
String key, value;
String ssid;
String pwd;
String ms;
String client_name;
String topic[] = {"picks", "warp", "fill", "others", "shiftSeconds", "runSeconds", "efficiency", "state", "cause"};
JsonDocument doc;


String pubserverUrl;
int t;
int PICKS=0, WARP, FILL, OTHERS, TOTAL, CAUSE,SHIFT,RUN;
bool STATE = true;
float EFF;
int minutes,hours,seconds;
// const char* pubserverUrl = ""; 
unsigned long previousMillis = 0;  
const long interval = 15000;  



WiFiClient espClient;
PubSubClient client(espClient);

struct change
{
  String E_ssid, E_pwd, E_ip;
  char E_client_name[32];
};

change acc;

String line()
{
  String str;
  while (1)
  {
    if (Serial.available())
    {
      str = Serial.readString();
      break;
    }
    delay(100);
  }
  return str;
}







void arrange()
{
  EEPROM.get(0, acc);
  ms = acc.E_ip;
  ssid = acc.E_ssid;
  pwd = acc.E_pwd;
  client_name=acc.E_client_name;
}

void wifi()
{
  WiFi.disconnect(true);
  Serial.print("Enter Your Hotspot name : ");
  ssid = line();
  Serial.println(ssid);
  Serial.print("Enter You Password : ");
  pwd = line();
  Serial.println(pwd);
  WiFi.begin(ssid, pwd);
}

void set()
{
  acc.E_ssid = ssid;
  acc.E_pwd = pwd;
  acc.E_ip = ms;

  strncpy(acc.E_client_name, client_name.c_str(), sizeof(acc.E_client_name));
  EEPROM.put(0, acc);
  EEPROM.commit();
}

void server()
{
  Serial.print("Enter Your Broker IP : ");
  ms = line();
  Serial.println(ms);
  Serial.print("Enter Your Name : ");
  client_name = line();
  Serial.println(client_name);
}

void EEP_value()
{

  Serial.println("-----------------------------------------------------------------------------");
  if (EEPROM.read(0) == 255)
  {
    Serial.println("EEPROM is empty please enter the all value manually");
    wifi();
    server();
    set();
    Serial.println("-----------------------------------------------------------------------------");
  }

  EEPROM.get(0, acc);

  Serial.println("Hotspot name : " + acc.E_ssid);
  Serial.println("Password : " + acc.E_pwd);
  Serial.println("Borker IP : " + acc.E_ip);
}


void cloud_upload(String str,int v_values)
{
  doc[str]=v_values;
}

void cloud_uploadf(String str,float v_values)
{
  doc[str]=v_values;
}


void pref()
{
  String choice;
  Serial.println("If you want change Value Press: 1\nDon't wanna change Press any key");
  choice = line();
  if (choice == "1")
  {
    wifi();
    server();
    set();
  }
  else
    arrange();
  return;
}

char * topic_val(String top)
{
  static char values[20];
  if(top == "picks")
  {
  cloud_upload(top,PICKS);
  
  sprintf(values,"%d",PICKS);
  }

  if(top== "warp")
  {
  cloud_upload(top,WARP);
  sprintf(values,"%d",WARP);
  }
  if(top=="fill")
  {
  cloud_upload(top,FILL);
  sprintf(values,"%d",FILL);
  }

  if(top=="others")
  {
  cloud_upload(top,OTHERS);
  sprintf(values,"%d",OTHERS);
  }
  
  if(top=="total")
  {
  cloud_upload(top,TOTAL);
  sprintf(values,"%d",TOTAL);
  }
  
  if(top=="shiftSeconds")
  {
    // cloud_upload(top,SHIFT);
    if(SHIFT>=60)
    {
      seconds = SHIFT % 60;
      minutes = SHIFT/60;

      if(minutes>=60)
      {
        minutes = SHIFT%3600;
        hours = hours /3600;
      }
    }
    else
    {
      seconds=SHIFT;
    }
    sprintf(values,"%02d:%02d:%02d",hours,minutes,seconds);
    cloud_upload(top,SHIFT);
  }
  
  
  if(top== "runSeconds")
  {
    if(RUN>=60)
    {
      seconds = RUN % 60;
      minutes = RUN/60;

      if(minutes>=60)
      {
        minutes = RUN%3600;
        hours = hours /3600;
      }
    }
    else
    {
      seconds=RUN;
    }
    sprintf(values,"%02d:%02d:%02d",hours,minutes,seconds);
    cloud_upload(top,RUN);
  }

  
  if(top=="efficiency")
{
  cloud_uploadf(top,EFF);
  dtostrf(EFF,6,2,values);
}
  
  if(top=="state")
  {
  // cloud_upload(top,STATE);
  doc[top] = int(STATE);
  sprintf(values,"%d",STATE);
  }
  
  if(top=="cause")
  {
    cloud_upload(top,CAUSE);
    sprintf(values,"%d",CAUSE);
  }
  return values;
}


void connect() {
  while (!client.connected())
  client.connect(client_name.c_str(), "bharani", "1234");
  // for(int i=0;i<10;i++)
  client.subscribe("details");
  Serial.println("Client connected");
}

void callback(char* topic, byte* payload, unsigned int length) {
  doc.clear();
  String message;
  Serial.print("Message arrived [");
  Serial.print(topic);
  Serial.print("] ");
  for (int i = 0; i < length; i++) 
  {
    message += (char)payload[i];
    Serial.print((char)payload[i]);
  }
  Serial.println();
  Serial.println(message);
  deserializeJson(doc,message);
  Serial.println(doc["RF_id"].as<String>());
  Serial.println(doc["name"].as<String>());
  Add_new(doc["RF_id"].as<String>(),doc["name"].as<String>());
}




void store(String data)
{
  int sep1, sep2;
  int startIndex = 0;
  String key, value;

  while (true)
  {
    sep1 = data.indexOf(':', startIndex);
    sep2 = data.indexOf(',', startIndex);
    if (sep2 == -1)
      sep2 = data.length();

    key = data.substring(startIndex, sep1);
    value = data.substring(sep1 + 1, sep2);
   

    if (key == "picks")
      PICKS = value.toInt();
    else if (key == "warp")
      WARP = value.toInt();
    else if (key == "fill")
      FILL = value.toInt();
    else if (key == "others")
      OTHERS = value.toInt();
    else if (key == "total")
      TOTAL = value.toInt();
    else if (key == "shift")
      SHIFT = value.toInt();
    else if (key == "run")
      RUN = value.toInt();
    else if (key == "eff")
      EFF = value.toFloat();
    else if (key == "state")
      STATE = true;
    else if (key == "cause")
      CAUSE = value.toInt();

    if (sep2 >= data.length())
      break;

    startIndex = sep2 + 1;
  }
  // Serial.println("Value stored");
}


int jsons(String str)
{
    HTTPClient http;
    // http.begin(serverUrl);
    // http.begin("http://192.168.1.38:8094/hms-rest-api/api/looms");
    http.begin("http://192.168.0.45:5000/api/data");
    // http://192.168.1.38:8094/hms-rest-api/api/data
    http.addHeader("Content-Type", "application/json");
    int httpResponseCode = http.POST(str);
    //SerialBT.println(httpResponseCode);
    // Serial.println(httpResponseCode);
    http.end();
    return httpResponseCode;
}







void setup() 
{
  pinMode(led,OUTPUT);
  Serial.begin(115200);
  NewSerial.begin(57600,SERIAL_8N1,16,17);
  SPIFFS.begin();
  EEPROM.begin(512);
  WiFi.mode(WIFI_STA);
  EEP_value();
  pref();
  EEPROM.get(0,acc);
  WiFi.begin(acc.E_ssid.c_str(),acc.E_pwd.c_str());
  while (WiFi.status() != WL_CONNECTED) 
  {
    Serial.print(".");
    delay(2000);
    t++;
    if (t == 5) 
    {
      Serial.println();
      wifi();
      t = 0;
    }
  }
  create_task();

  Serial.println("Connected");
  Serial.print("IP address : ");
  Serial.println(WiFi.localIP());
  set();

  // Serial.print("\nPlease Enter the Server URL : ");
  
  // pubserverUrl = line();
  // Serial.println(pubserverUrl);
  

  // // http.end();



  // Serial.println();
  
  client.setServer(ms.c_str(), 1883);
  client.setCallback(callback);
}



void loop()
{
    if (!client.connected())
    connect();
    if(NewSerial.available())
    {
    String input;
    while(1)
    {
      input =NewSerial.readStringUntil('\n');
      if(input.length()>0)
      {
        // Serial.println("Data arriver : " + input);
      break;
      }
    }
    store(input);
    }
  String val;
 if(millis()-pubmillis>1000)
 {
  doc.clear();
  for(int i =0;i<9;i++)
  {
    
    val=topic_val(topic[i]);
    client.publish(topic[i].c_str(), val.c_str(),true);
  }
  pubmillis = millis();
 }
  if(millis()-previousMillis >= interval)
  {
    int M_id = (rand() % 25) + 1;
    doc["machineId"] = M_id;
      previousMillis=millis();
      String jsonString;
      serializeJson(doc, jsonString);
      Serial.println(jsonString);
      int response = jsons(jsonString);
      // if(response!=200)
      // {
      //   failed(jsonString);
      // }
      // else
      // {
  
      //   Send();
      // }
  }
  client.loop();
  delay(100);
}
