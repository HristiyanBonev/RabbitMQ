#!/usr/bin/env python
import pika
import sys
import os
import time

os.chdir('/home/hristiyan-bonev/Desktop')

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='topic_cars',
                         exchange_type='topic')

routing_key = sys.argv[1] if len(sys.argv) > 2 else '*.garage'
message = ' '.join(sys.argv[2:]) or 'Hello World!'

with open('numbers.txt', 'r+') as f:
    for row in f.readlines():
            channel.basic_publish(
                exchange='topic_cars',
                routing_key=routing_key,
                body=row)
    print(" [x] Sent %r:%r" % (routing_key, row))
connection.close()
