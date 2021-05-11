from ruuvitag_sensor.ruuvi import RuuviTagSensor
import json
import datetime
from iot_core import get_client

def disconnect_cb():
    global reconnect
    reconnect = True

client = get_client(disconnect_cb)
jwt_issue_time = datetime.datetime.utcnow()
last_call_time = datetime.datetime.utcnow()
call_interval_mins = 1
jwt_exp_mins = 20
reconnect = False

def handle_data(found_data):
    global client
    global reconnect
    global jwt_issue_time
    global jwt_exp_mins
    global last_call_time
    global call_interval_mins

    seconds_since_last_call = (datetime.datetime.utcnow() - last_call_time).seconds
    if seconds_since_last_call < 60 * call_interval_mins:
        return
    
    mqtt_topic = "/devices/{}/{}".format("ruuvi_proxy", "events")    
    payload = json.dumps(found_data[1])

    seconds_since_issue = (datetime.datetime.utcnow() - jwt_issue_time).seconds
    if (seconds_since_issue > (60 * jwt_exp_mins)) or reconnect:
        print("Refreshing token after {}s".format(seconds_since_issue))
        jwt_issue_time = datetime.datetime.utcnow()
        client.loop()
        client.disconnect()
        try:
            client = get_client(disconnect_cb)
            reconnect = False
        except:
            print("Couldn't connect")


    print("Publishing message: '{}'".format(payload))
    
    client.publish(mqtt_topic, payload, qos=1)
    client.loop()

    last_call_time = datetime.datetime.utcnow()


RuuviTagSensor.get_datas(handle_data)


#  projects/ruuvicloud/subscriptions/gcf-ruuvidataendpoint-dev-ruuvidata-europe-west1-ruuvidata 