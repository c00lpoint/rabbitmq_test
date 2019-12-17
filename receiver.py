import pika
import os
import sys

MQ_SERVER = os.environ.get('MQ_SERVER', default='localhost')
MQ_AUTH_USER = os.environ.get('MQ_AUTH_USER', default='guest')
MQ_AUTH_PASS = os.environ.get('MQ_AUTH_PASS', default='guest')


def callback(ch, method, properties, body):
    print("[x] {}".format(body.decode()))


def create_cannel():
    credential = pika.PlainCredentials(MQ_AUTH_USER, MQ_AUTH_PASS)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=MQ_SERVER, credentials=credential))
    return connection.channel()


def start_listing(listener):
    channel = create_cannel()
    channel.queue_declare(queue=listener)
    _start_consuming(listener, channel)


def start_receiving(channel_id):
    channel = create_cannel()
    channel.exchange_declare(exchange=channel_id, exchange_type='fanout')
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange=channel_id, queue=queue_name)
    _start_consuming(queue_name, channel)


def _start_consuming(queue_name, channel):
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    print('[*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == "__main__":
    err = True
    if len(sys.argv) > 2:
        tp = sys.argv[1]
        listener = sys.argv[2]
        if tp.lower() == 'listen':
            start_listing(listener)
            err = False
        elif tp.lower() == 'receive':
            start_receiving(listener)
            err = False
    if err:
        print('Argument Error, argument format: [listen|receive]  [listener|channel_id]')
