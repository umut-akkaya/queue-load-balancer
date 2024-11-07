import pika
import time
import random, os, sys

rhost = os.environ["RABBITMQ_HOST"]
rqueue = os.environ["RABBITMQ_LB_QUEUE_NAME"]
lbqueuelist = os.environ["RABBITMQ_REGISTERED_QUEUE_LIST"]
ruser = os.environ["RABBITMQ_USER"]
rsecret = os.environ["RABBITMQ_PASSWORD"]
r_time = os.environ["LB_WAIT_INTERVAL"]

credentials = pika.PlainCredentials(ruser, rsecret)
parameters = pika.ConnectionParameters(rhost,
                                       5672,
                                       '/',
                                       credentials)
try:
    connection = pika.BlockingConnection(parameters=parameters)
except Exception as e:
    print(f"Error has been detected: {str(e)}")
    sys.exit()
channel = connection.channel()
lb_queue = channel.queue_declare(queue=rqueue, passive=False, durable=True)
listener_list = lbqueuelist.split(',')
cursor = 0
while (True):
    lb_queue = channel.queue_declare(queue=rqueue, passive=True, durable=True)
    if (lb_queue.method.message_count <= len(listener_list) - 1):
        method_frame, header_frame, mbody = channel.basic_get(listener_list[cursor])
        
        if method_frame:
            channel.basic_publish(exchange='',
                        routing_key=rqueue,
                        body=mbody)
            channel.basic_ack(method_frame.delivery_tag)
            cursor = cursor + 1
            cursor = cursor % len(listener_list)
            print(f"Message from queue {listener_list[cursor]} has been redirected, Cursor switched to {listener_list[(cursor + 1) % len(listener_list)]}, lb queue size {str(lb_queue.method.message_count + 1) }")
            time.sleep(float(r_time))
        else:
            print(f"No message in queue {listener_list[cursor]}, Cursor switched to {listener_list[(cursor + 1) % len(listener_list)]}")
            cursor = cursor + 1
            cursor = cursor % len(listener_list)
            time.sleep(float(r_time))
            continue
    else:
        print("LB Queue size is at Max")
        time.sleep(float(r_time))
        continue


