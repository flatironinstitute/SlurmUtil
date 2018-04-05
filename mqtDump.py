
import sys
import paho.mqtt.client as mqtt

hn, msgs = sys.argv[1:]
msgs = int(msgs)
if msgs == 0: sys.exit(0)

def on_connect(client, userdata, flags, rc):
    mqtt_client.subscribe("cluster/hostprocesses/%s"%hn)

mc = 0
def on_message(client, userdata, msg):
    global mc
    print ('%4d ================================================================================'%mc)
    mc += 1
    print (msg.payload)
    if mc == msgs: sys.exit(0)

mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.connect("mon5.flatironinstitute.org")

mqtt_client.loop_forever()
        
