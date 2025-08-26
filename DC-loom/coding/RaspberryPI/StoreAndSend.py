from xmlrpc.client import ProtocolError
from flask import Flask, request, jsonify
from datetime import datetime
import paho.mqtt.client as mqtt
import os
import logging
import threading
import time
import requests
import json

app = Flask(__name__)
folder_path = r"D:\t"

# In-memory store
received_data = []
file_count = 0

url = ""

broker = "localhost"
port = 1883
topic = "details"



def fail_post(file_path):
    global url
    files = os.listdir(file_path)
    for i in files:
        full_path = os.path.join(file_path,i)
        if os.path.isfile(full_path):
            if full_path.endswith(".json"):
                with open(full_path,"r") as json_file:
                    data = json.load(json_file) 
                response = send(data)
                if response in (200,201):
                    os.remove(full_path)
                else:
                    return
        else: 
            if i == "Employee" :
                url = "http://127.0.0.1:5000/Employee"
            if i == "Loom":
                url = "http://127.0.0.1:5000/loom"
            fail_post(full_path)
            if len(os.listdir(full_path))==0:
                os.rmdir(full_path)


def send(data):
    try:
        # url = "https://webhook.site/00420d0e-970f-4cf5-8e57-d7a02eba56b5"
        response = requests.post(url, json=data)
        return response.status_code
    except requests.exceptions.ConnectionError as e:
        logging.warning(e)
        return -200
    
    
def store_data(data):
    global url
    store_folder_path = r"D:\t" + "\\store_data"
    os.makedirs(store_folder_path,exist_ok=True)
    if(request.path == '/api/Employee'):
        store_folder_path = store_folder_path + "\Employee"
        os.makedirs(store_folder_path,exist_ok=True)
        url = "http://127.0.0.1:5000/Employee"
    if(request.path == '/api/data'):
        store_folder_path = store_folder_path + "\Loom"
        os.makedirs(store_folder_path,exist_ok=True)
        url = "http://127.0.0.1:5000/loom"
    store_file_path = store_folder_path +"\\" + str(data.get("machineId"))
    os.makedirs(store_file_path,exist_ok=True)
    store_file_date = store_file_path +"\\"+datetime.now().strftime("%Y-%m-%d")
    os.makedirs(store_file_date,exist_ok=True)
    store_file_name = store_file_date+"\\"+datetime.now().strftime("%H-%M-%S")+".json"
    with open(store_file_name,"w") as file:
        json.dump(data,file,indent=4)
    # file_name  = os.path.join(folder_path,str(5),str(datetime.now().strftime("%H-%M-%S"))+".json")


def fail_data(data):
    fail_folder_path = r"D:\t" + "\\fail_data"
    os.makedirs(fail_folder_path,exist_ok=True)
    if(request.path == '/api/Employee'):
        fail_folder_path = fail_folder_path + "\Employee"
        os.makedirs(fail_folder_path,exist_ok=True)
    if(request.path == '/api/data'):
        fail_folder_path = fail_folder_path + "\Loom"
        os.makedirs(fail_folder_path,exist_ok=True)
    fail_file_path = fail_folder_path +"\\" + str(data.get("machineId"))
    os.makedirs(fail_file_path,exist_ok=True)
    fail_file_date = fail_file_path +"\\"+datetime.now().strftime("%Y-%m-%d")
    os.makedirs(fail_file_date,exist_ok=True)
    fail_file_name = fail_file_date+"\\"+datetime.now().strftime("%H-%M-%S")+".json"
    with open(fail_file_name,"w") as file:
        json.dump(data,file,indent=4)


def register(data):
    client = mqtt.Client()
    client.username_pw_set("bharani","1234")
    client.connect(broker,port,60)

    result = client.publish(topic,json.dumps(data))
    print(result)

    status = result[0]
    if status == 0:
        print(f"✅ Message published successfully to topic {topic}")
    else:
        print(f"❌ Failed to publish message to topic {topic}")
    print("Published",json.dumps(data))



@app.route('/api/data', methods=['POST'])
@app.route('/api/NewRegister', methods=['POST'])
@app.route('/api/Employee', methods=['POST'])



def receive_data():
    logging.basicConfig(level=logging.INFO , filename=r"C:\Users\advan\OneDrive\Documents\PlatformIO\python\Projects\logging\log.txt",filemode="w")

    if request.is_json:
        data = request.get_json()
        if request.path == '/api/NewRegister':
            register(data)
            return jsonify({"status": "success", "message": "Data received"}), 200
        # Current timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        store_data(data)
        if send(data) not in (200,201):
            fail_data(data)
        else:
            fail_post(folder_path +"\\fail_data")
        # Print received data
        print("\n" + "="*50)
        print(f"Received data from client at {timestamp}:")
        print("-"*50)
        for key, value in data.items():
            print(f"{key:>15}: {value}")
        print("="*50 + "\n")

        # Save to memory
        received_data.append(data)
      
        return jsonify({"status": "success", "message": "Data received"}), 200
    else:
        print("\n" + "!"*50)
        print("ERROR: Received non-JSON data")
        print("!"*50 + "\n")
        return jsonify({"status": "error", "message": "Request must be JSON"}), 400



@app.route('/api/data', methods=['GET'])
@app.route('/api/NewRegister', methods=['GET'])
@app.route('/api/Employee', methods=['GET'])
def get_data():
    return jsonify(received_data), 200


# ---- Extra: Auto POST test when server starts ----

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)