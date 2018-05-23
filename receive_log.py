#!/usr/bin/env python
import pika
import sys
import time

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='calculations',
                         exchange_type='topic')
# channel.exchange_declare(exchange='topic_cars_results',
                        # exchange_type='topic')

result1 = channel.queue_declare(queue='math')

binding_keys = sys.argv[1:]
if not binding_keys:
    sys.stderr.write("Usage: %s [binding_key]...\n" % sys.argv[0])
    sys.exit(1)

for binding_key in binding_keys:
    channel.queue_bind(exchange='calculations',
                       queue='math',
                       routing_key=binding_key)

print(' [*] Waiting for logs. To exit press CTRL+C')


def callback(ch, method, properties, body):
    # import pdb; pdb.set_trace()
    # time.sleep(2)
    try:
        body = [int(x) for x in body.strip()]
        result = reduce(lambda x, y: x * y, body)
        print(result)
        # if result in range(100,1000):
        #     channel.basic_publish(
        #         exchange='topic_cars_results',
        #         routing_key='calculate.result',
        #         body=result)
            # print('Hit! Sent to calculate.result query')
    except Exception:
        print('Please provide only numbers')
    channel.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_consume(callback,
                      queue='math',
                      )


channel.start_consuming()
