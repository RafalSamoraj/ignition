import requests
import datetime
import time
import paho.mqtt.client as mqtt
#import json
from json import dumps

login_url = "https://pc0244907.danfoss.net:5001/api/auth/signin"
userAndPass = {"username" : "bob", "password" : "Pass123$"}
robots = ["1", "2", "3", "4", "5", "6", "7"]
isRESTAPIconnected = False
isMQTTconnected = False

requests.packages.urllib3.disable_warnings() #disable warnings

while True:

    #connection with REST API
    while not isRESTAPIconnected:

        session = requests.Session() #open session
        response = session.post(login_url, json = userAndPass, verify = False) #logging
        print("REST API connecting...")

        if response.status_code == 200:
            print("REST API connected")
            isRESTAPIconnected = True
        else:
             print("REST API connection error")  

        time.sleep(5)

    #connection with MQTT broker
    while not isMQTTconnected:

        try:
            print("MQTT conecting...")           
            client = mqtt.Client()
            client.connect("10.3.96.110", 1883, 360)
            isMQTTconnected = True
            print("MQTT connected")
        except Exception:
            isMQTTconnected = False
            print("MTQQ connection error")
            time.sleep(5)

    #download data from REST API and upload to MQTT broker
    if isRESTAPIconnected and isMQTTconnected:

        #get robots data
        for robotNumber in robots:

            robot_url = "https://pc0244907.danfoss.net:5001/api/robots/" + robotNumber
            response = session.get(robot_url, verify = False)            

            if response.status_code != 200:
                isRESTAPIconnected = False
                print("REST API data download fail")
                break
            
            #data extraction
            rawData = response.json()            

            deviceInfo = rawData["deviceInfo"]
            id = deviceInfo["id"]
            modelName = deviceInfo["modelName"]

            deviceStatus = rawData["deviceStatus"]
            errorNumber = deviceStatus["errorNumber"]
            connectionStatus = deviceStatus["connectionStatus"]
            operationStatus = deviceStatus["operationStatus"]

            melfaSmartPlus = rawData["melfaSmartPlus"]
            totalScore = melfaSmartPlus["totalScore"]
            maintenancePartsConsumptionDegree = totalScore["maintenancePartsConsumptionDegree"]
            overhaulPartsConsumptionDegree = totalScore["overhaulPartsConsumptionDegree"]
            upToMaintenance = totalScore["upToMaintenance"]
            gearAbnormality = totalScore["gearAbnormality"]
            encoderAbnormality = totalScore["encoderAbnormality"]
            batteryAbnormality = totalScore["batteryAbnormality"]
            
            #prepare data to send to MQTT
            toMQTTbroker = {} #create dictionary
            toMQTTbroker.update({
                "id": id,
                "modelName": modelName,
                "errorNumber": errorNumber,
                "connectionStatus": connectionStatus,
                "operationStatus": operationStatus,
                "maintenancePartsConsumptionDegree": maintenancePartsConsumptionDegree,
                "overhaulPartsConsumptionDegree": overhaulPartsConsumptionDegree,
                "upToMaintenance": upToMaintenance,
                "gearAbnormality": gearAbnormality,
                "encoderAbnormality": encoderAbnormality,
                "batteryAbnormality": batteryAbnormality
                                }) #put data into dictionary                                
            toMQTTbroker = dumps(toMQTTbroker) #convert to JSON string            
            
            #send to MQTT broker
            try:                            
                topic = "me2robot/" + robotNumber
                message = toMQTTbroker
                client.publish(topic, message)
                print("MQTT data uploaded")
            except Exception:
                isMQTTconnected = False
                print("MQTT data upload fail")

    print("Cycle end")
    time.sleep(60)    