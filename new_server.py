#!/usr/bin/#!/usr/bin/env python3
import sys
import pika


class Consumer(object):
    _HOST = 'localhost'
    _EXCHANGE = 'calculations'
    _EXCHANGE_TYPE = 'topic'
    _WORKING_QUEUE = 'math'
    _RESULTS_QUEUE = 'math.results'

    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._HOST))
        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange=self._EXCHANGE,
                                      exchange_type=self._EXCHANGE_TYPE)
        self.channel.queue_declare(queue=self._WORKING_QUEUE)
        self.channel.queue_declare(queue=self._RESULTS_QUEUE)
        self.working_binding_key = sys.argv[1]
        self.channel.queue_bind(exchange=self._EXCHANGE,
                                queue=self._WORKING_QUEUE,
                                routing_key=self.working_binding_key)
        self.channel.queue_bind(exchange=self._EXCHANGE,
                                queue=self._RESULTS_QUEUE,
                                routing_key='math.calculated'
                                )

        print('Consumer created: Exchange:%s, Queue:%s' % (self._EXCHANGE,
                                                           self._WORKING_QUEUE))

    def calculate_message(body):
        body = [int(x) for x in body.strip()]
        result = reduce(lambda x, y: x * y, body)
        return result

    def _publish(self, queue, msg):
        result = self.calculate_message(msg)
        if result < 1000:
            routing_key = 'math.calculated'
        else:
            routing_key = 'test'
        self.channel.basic_publish(exchange='calculations',
                                       routing_key=routing_key,
                                       body=msg,
                                       properties=pika.BasicProperties(
                                            delivery_mode=2,  # persistent
                                            ))


    def message_callback(self, ch, method, properties, body):
        try:
            # import pdb; pdb.set_trace()
            self._publish('math', body)
            # print
        except Exception as e :
            print(e)

    def consume_channel(self, queue):
        self.channel.basic_consume(self.message_callback, queue=queue)
        print(' [*] Waiting for data. To exit press CTRL+C')
        self.channel.start_consuming()


co = Consumer()
co.consume_channel('math')
