import pika
import os
import sys
import re
from time import sleep

MQ_SERVER = os.environ.get('MQ_SERVER', default='localhost')
MQ_AUTH_USER = os.environ.get('MQ_AUTH_USER', default='guest')
MQ_AUTH_PASS = os.environ.get('MQ_AUTH_PASS', default='guest')

WAITING_MESSAGE = '[*] Waiting for messages. To exit press CTRL+C'
SENDER_HEADER_PATTERN_STR = r'^@\w+?@:\s*\[task\]'


def callback(ch, method, properties, body):
    body_str = body.decode()
    print(f"[x] {body.decode()}")
    task_header = re.findall(SENDER_HEADER_PATTERN_STR, body_str, flags=re.IGNORECASE)
    if task_header:
        header_idx = len(task_header[0])
        task_body = body_str[header_idx:].strip()
        print(f'Start [{task_body}]')
        sleep(1)
        print(f'Done [{task_body}]')


def create_cannel():
    credential = pika.PlainCredentials(MQ_AUTH_USER, MQ_AUTH_PASS)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=MQ_SERVER, credentials=credential))
    return connection.channel()


def start_listing(listener):
    channel = create_cannel()
    channel.queue_declare(queue=listener)
    channel.basic_consume(queue=listener, on_message_callback=callback, auto_ack=True)
    print(WAITING_MESSAGE)
    channel.start_consuming()


def start_receiving(channel_id):
    channel = create_cannel()
    channel.exchange_declare(exchange=channel_id, exchange_type='fanout')
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange=channel_id, queue=queue_name)
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    print(WAITING_MESSAGE)
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
