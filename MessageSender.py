'''
Created on Aug 16, 2013

@author: family
'''
import pika



class MessageSender(object):

    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
               'localhost'))
        self.channel = self.connection.channel()

        self.channel.queue_declare(queue='hello')
        
    def send(self,body):
        self.channel.basic_publish(exchange='',
                      routing_key='hello',
                      body=body)
        
    def close(self):
        self.connection.close()

        