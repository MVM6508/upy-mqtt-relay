"""Main module of MQTT relay switch"""

import machine
import ujson 
import time

from mqtt import MQTTClient 
import config
import relay_button

def subscribe_commands(topic, msg):
    """Callback function to handle messages on specific topic"""

    try:
        values = ujson.loads(msg)
        send_relay_command(values["bid"], values["on"])
        print("Received 'on' command for button ID {}".format(values["bid"]))
    except (ValueError, KeyError) as e:
        print("Malformed message - ", msg, "(topic", topic, "). Ignoring")

def connect_mqtt():
    """Connect to MQTT server and subscribe to command topics"""

    client = MQTTClient(config.MQTT_CLIENT, config.MQTT_SERVER, port=config.MQTT_PORT, user=config.MQTT_USER, password=config.MQTT_PASSWD) 
    client.set_callback(subscribe_commands) 
    client.connect()
    print("Connected MQTT server at {}:{}".format(config.MQTT_SERVER, config.MQTT_PORT))
    client.subscribe(topic=config.TOPIC_COMMANDS) 
    print("Listening on topic '{}'".format(config.TOPIC_COMMANDS))
    return client

def restart_and_reconnect():
    """On failure, reboot the machine to start fresh"""
    
    print('Failed to connect to MQTT broker. Reconnecting ...')
    time.sleep(5)
    machine.reset()

def init_relay_buttons():
    """return array of buttons used"""

    rv = list()

    for i in range(4):
        rb = relay_button.RelayButton(i+1, config.RELAY_PINS[i], 0)
        rv.append(rb)
    
    return rv

def send_relay_command(relay_id, on_command):
    """Send the command to appropriate relay. If relay id is 0, send to all"""

    if relay_id == 0:
        for relay in relays:
            relay.button_push(on_command)
            client.publish("{0}/{1}".format(config.TOPIC_STATUS, str(relay.button_id)), "off")
        time.sleep(config.BUTTON_PULSE_TIMEOUT)
        for relay in relays:
            client.publish("{0}/{1}".format(config.TOPIC_STATUS, str(relay.button_id)), "on")
            
    else:
        for relay in relays:
            if relay_id == relay.button_id:
                relay.button_push(on_command)
                client.publish("{0}/{1}".format(config.TOPIC_STATUS, str(relay.button_id)), "off")
                time.sleep(config.BUTTON_PULSE_TIMEOUT)
                client.publish("{0}/{1}".format(config.TOPIC_STATUS, str(relay.button_id)), "on")

def publish_status():
    """Publish status of all relays to the notification topic. Send this as a callback to
    each relay"""

    # Since this setup is used as a reset function (NC) controlled outlets are on when relays are idle so we publish "on" so Home Assistant to on at reboot 
    for relay in relays:
        client.publish("{0}/{1}".format(config.TOPIC_STATUS, str(relay.button_id)), "on")

# init relay array
relays = init_relay_buttons()

try:
    client = connect_mqtt()
except OSError as e:
    restart_and_reconnect()
    
#init timer
timer_start = time.time()

while True:  
    try:
        client.check_msg()
        machine.idle()
        #Publish relays state "on" to notify controlsystem if it is rebooted
        if time.time() >= timer_start + (12 * config.BUTTON_PULSE_TIMEOUT):
            publish_status()
            timer_start = time.time()
        for relay in relays:
            relay.idle()
    except OSError as e:
        restart_and_reconnect()
