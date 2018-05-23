import pika
import sys

class Producer(object):

    def __init__(self):
        print('Initializting MQ producer')
        self._connect()

    def _connect(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='calculations',
                                      exchange_type='topic'
                                      )
        self.channel.queue_declare(queue='math')
        try:
            self.binding_key = sys.argv[1]
        except Exception:
            sys.stderr.write("Usage: %s [binding_key]...\n" % sys.argv[0])
            sys.exit(1)
        self.channel.queue_bind(exchange='calculations',
                                queue='math',
                                routing_key=self.binding_key)

    def publish(self, queue, msg):
        print('Publishing message to queue %s: %s' % (queue, msg))
        try:
            self._publish(queue, msg)
        except Exception:
            print('Got exception when trying to publish MQ message, '
                  'reconnecting')
            self._connect()
            self._publish(queue, msg)

    def _publish(self, queue, msg):
        self.channel.basic_publish(exchange='calculations',
                                   routing_key=self.binding_key,
                                   body=msg,
                                   properties=pika.BasicProperties(
                                      delivery_mode=2,  # persistent
                                   ))

    def close(self):
        try:
            self.connection.close()
        except pika.exceptions.ConnectionClosed:
            pass


p = Producer()
p.publish(queue='math', msg=str(sys.argv[2]))
