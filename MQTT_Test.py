from paho.mqtt import client as mqtt_client
import json

BROKER_HOSTNAME = 'pump-controller-1'
BROKER_PORT = 1883


def on_message(client: mqtt_client.Client, _, message: mqtt_client.MQTTMessage):
    print(f'{message.topic}: {message.payload}')


def main():

    client = mqtt_client.Client('python_controller')
    client.on_message = on_message
    client.on_connect = lambda *_: print('Successfully connected to MQTT broker')
    client.connect(BROKER_HOSTNAME, BROKER_PORT)

    client.loop_start()
    client.subscribe('+/mass')

    try:
        while True:
            topic = input('Enter topic:')
            message = input('Enter message:')
            client.publish(topic, message)
    except KeyboardInterrupt:
        client.disconnect()
    except Exception as ex:
        print(type(ex))
        print(ex)


if __name__ == '__main__':
    main()
