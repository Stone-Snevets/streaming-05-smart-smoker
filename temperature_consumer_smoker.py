"""
    This program will listen for messages to arive from the queue and process them

    Author: Solomon Stevens
    Date: May 24, 2024

    Copied and modified from streaming-04-multiple-consumers/v3_listening_worker.py
    
"""

# ===== Preliminary Stuff =====================================================

# Imports
import pika
import sys
from collections import deque
from util_logger import setup_logger

# Constants
HOST = 'localhost'
MAX_DEQUE_SPACE:int = 5 # 2 readings a minute for 2.5 minutes
QUEUE_NAME = 'str5_q_smoker'

# Setup custom logging
logger, logname = setup_logger(__file__)

# Create a deque window
window = deque(maxlen=MAX_DEQUE_SPACE)


# ===== Functions =============================================================

# --- Deque and Flag ---
def deque_and_flag(msg_body):
    """
    Function to append a new time and temperature to the end of a deque
    -> Converts temperature from a string to a float
    -> Checks if temperature drops by 15 degrees over a span of 2.5 minutes
    --> Sends a flag message if temperature does so

    Parameters:
        msg_body (str): the contents of the latest message

    """

    # Split the string
    #-> Use the apostrophe (') as a delimeter
    front_paren, timestamp, comma, temperature, back_paren = msg_body.split("'")

    # Cast the temperature to a float
    temperature = float(temperature)

    # Append the timestamp and temperature to the deque
    window.append(temperature)

    # Check for a drastic temperature drop
    #-> Make sure the smoker has been going on for at least 2.5 minutes (i.e. the deque is full)
    if len(window) == MAX_DEQUE_SPACE:
        # If full, subtract the last element of the deque from the first element
        #-> Earlier temp - latest temp
        temp_diff = window[0] - window[-1]

        # If the difference is greater than 15
        # TODO: > or >=
        if temp_diff >= 15.0:
            # If yes, we have an issue.  Throw up a flag message
            logger.info('\n\nFLAG: Significant Temperature Drop in Smoker')
            logger.info(f'\tOld Temperature:{window[0]}\n\tNew Temperature:{window[-1]}')
            logger.info(f'\tTimestamp: {timestamp}\n\n')

    

# --- Callback ---
def callback(ch, method, properties, body):
    """
    Function to define the behavior on how to receive a message

    Parameters:
        ch: the channel for receiving messages
        method: metadata about delivery
        properties: user-defined properties
        body: the actual message

    Read more about it here:
    https://stackoverflow.com/questions/34202345/rabbitmq-def-callbackch-method-properties-body

    """

    # Send the decoded body to the deque
    deque_and_flag(body.decode())

    # Acknowledge that the message is received and processed
    logger.info(f'Received and processed {body.decode()}')
    ch.basic_ack(delivery_tag = method.delivery_tag)



# --- Main ---
def main(host_name = 'localhost', queue_name = 'default_queue'):
    """
    Create a connection and channel to the queue and receive messages
    Program never ends until stopped by user (CTRL + C)

    Parameters:
        host_name (str): (Default: localhost): the host or IP address
        queue_name (str): (Default: default_queue): the name of the queue to connect to

    """

    # Create a connection
    try:
        conn = pika.BlockingConnection(pika.ConnectionParameters(host = host_name))

    except Exception as e:
        logger.error("ERROR: connection to RabbitMQ server failed.")
        logger.error(f"Verify the server is running on host: {host_name}.")
        logger.error(f"The error says: {e}")
        sys.exit(1)

    # Create a channel and connect it to the queue
    try:
        # Create a channel
        ch = conn.channel()

        # Declare the queue
        #-> Make the queue durable
        #-> Use the channel to do so
        ch.queue_declare(queue = queue_name, durable = True)

        # Limit the number of messages the worker can work with at one time
        ch.basic_qos(prefetch_count=1)

        # Configure the channel to listen to the correct queue
        #-> Let callback handle the acknowledging of messages
        ch.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)

        # Start consuming messages
        logger.info('Ready for action! Press CTRL + C to manually close the connection.')
        ch.start_consuming()
    
    except Exception as e:
        logger.error("ERROR: something went wrong.")
        logger.error(f"The error says: {e}")
        sys.exit(1)

    # If user manually ends the system
    except KeyboardInterrupt:
        logger.info('User interrupted continuous listening process')
        sys.exit(0)

    # Close the connection when we are done
    finally:
        logger.info('Closing Connection...')
        conn.close()

# ===== Main ==================================================================
if __name__ == '__main__':
    # Call the main function
    main(HOST, QUEUE_NAME)