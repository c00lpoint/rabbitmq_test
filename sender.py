import pika
import os
import sys

from contextlib import contextmanager

MQ_SERVER = os.environ.get('MQ_SERVER', default='localhost')
MQ_AUTH_USER = os.environ.get('MQ_AUTH_USER', default='guest')
MQ_AUTH_PASS = os.environ.get('MQ_AUTH_PASS', default='guest')


@contextmanager
def _open_connection():
    connection = None
    try:
        credential = pika.PlainCredentials(MQ_AUTH_USER, MQ_AUTH_PASS)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=MQ_SERVER, credentials=credential))
        yield connection
    finally:
        if connection:
            connection.close()


def send_message(message, *listeners):
    with _open_connection() as conn:
        channel = conn.channel()
        for lname in listeners:
            channel.queue_declare(queue=lname)
            channel.basic_publish(exchange='', routing_key=lname, body=message)
        print(f"[x] Sent '{message}' to {listeners}")


def broadcast_message(message, channel_id):
    with _open_connection() as conn:
        channel = conn.channel()
        channel.exchange_declare(exchange=channel_id, exchange_type='fanout')
        channel.basic_publish(exchange=channel_id, routing_key='', body=message)
        print(f"[x] Broadcast f{message} to {channel_id}")


if __name__ == '__main__':
    err = True
    if len(sys.argv) > 4:
        sender = sys.argv[1]
        tp = sys.argv[2]
        msg = sys.argv[3]
        std_msg = f"@{sender}@: {msg}"
        if tp.lower() == 'send':
            targets = sys.argv[4:]
            send_message(std_msg, *targets)
            err = False
        elif tp.lower() == 'post':
            target = sys.argv[4]
            broadcast_message(std_msg, target)
            err = False
    if err:
        print('Argument Error, argument format: [sender] [send|post] [message] [receiver1, ..., receiver2|channel_id]')