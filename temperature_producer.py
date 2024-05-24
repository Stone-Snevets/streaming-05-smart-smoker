"""
  Program to read in temperatures from provided CSV file.
  Will send temperatures to respective consumers based on
    what we are reading the temperatures of (smoker, foods).

  Author: Solomon Stevens
  Date: May 31, 2024

  Copied and modified from streaming-04-multiple-consumers/v3_emitter_of_tasks.py

  Basic Steps:
  1. If 'SHOW_WEB' is set to True, ask user if they want to open RabbitMQ Admin website
     -> If yes, open website
  2. Open the CSV file
  3. Create a CSV reader object
  4. Create a connection and channel to stream the data in the CSV file
  5. Declare three queues
  6. Create three tuples from the CSV file
     -> (Time, Temperature of the smoker)
     -> (Time, Temperature of the first food)
     -> (Time, Temperature of the second food)
  7. Send each tuple to their respective queue

"""

# ===== Preliminary Stuff =====================================================

# Imports
import csv
import logging
import pika
import sys
import webbrowser

import pika.exceptions

# Constants
HOST = 'localhost'
QUEUE_NAME_SMOKER = 'str5_q_smoker'
QUEUE_NAME_FOOD_1 = 'str5_q_food_1'
QUEUE_NAME_FOOD_2 = 'str5_q_food_2'
FILE_TO_READ = 'smoker-temps.csv'
SHOW_WEB = False

# Create Logger
logging.basicConfig(level=logging.INFO, format = "%(asctime)s - %(level)s - %(message)s")



# ===== Functions =============================================================

# --- Website Offerer ---
def offer_website():
    """
    Function to give user option to open RabbitMQ admin website

    """
    # Get user input
    user_input = input('Do you want to open RabbitMQ to monitor queues? ("y" or "n")\n')

    # Check if user agrees
    if user_input.lower() == 'y':
        # If yes, go to website
        webbrowser.open_new("http://localhost:5672/#/queues")


# --- Message Sender ---
def send_msg(host:str,
             queue_name_1:str,
             queue_name_2:str,
             queue_name_3:str,
             file_name):
    """
    Function to send messages to the queue
    -> This process runs and finishes

    Parameters:
        host (str): the hostname or IP address
        queue_name_1 (str): the name of the first queue
        queue_name_2 (str): the name of the second queue
        queue_name_3 (str): the name of the third queue
        file_name (str): the name of the file to open

    """

    logging.info(f'opened send_msg({host}, {queue_name_1}, {queue_name_2}, {queue_name_3}, {file_name})')

    # Open the file
    with open(FILE_TO_READ, 'r') as input_file:

        # Create a reader object
        reader = csv.reader(input_file)

        # Remove the header row
        header = next(reader)

        try:
            # Create a blocking connection to the RabbitMQ server
            conn = pika.BlockingConnection(pika.ConnectionParameters(host))

            # Use the connection to create a communication channel
            ch = conn.channel()

            # Delete existing queues
            #-> This removes whatever contents may be left from a
            #   previous declaration of the queue.
            ch.queue_delete(queue=queue_name_1)
            ch.queue_delete(queue=queue_name_2)
            ch.queue_delete(queue=queue_name_3)

            # Use the channel to declare durable queues
            #-> A durable queue will survive a RabbitMQ server restart
            #   and help ensure messages are processed in order.
            #-> Messages will not be deleted until the consumer acknowledges.
            ch.queue_declare(queue=queue_name_1, durable=True)
            ch.queue_declare(queue=queue_name_2, durable=True)
            ch.queue_declare(queue=queue_name_3, durable=True)

            # For each row in the file
            for row in reader:
                
                # Assign each element of the row to a variable
                Time, Temp_Smoker, Temp_Food_1, Temp_Food_2= row
                
                # If the temperature is not registered
                # Then:
                # Create tuples of each channel with the time
                # Use the channel to publish each tuple to a queue
                #-> The first tuple goes to the first queue
                #   The second tuple goes to the second queue
                #   The third tuple goes to the third queue
                #   The tuples are sent as a string to avoid syntax errors
                #-> Every message passes through an exchange
                if Temp_Smoker != '':
                  tuple_smoker = (Time, Temp_Smoker)
                  ch.basic_publish(exchange="", routing_key=queue_name_1, body=str(tuple_smoker))
                  logging.info(f'Sent {tuple_smoker} to {queue_name_1}')

                if Temp_Food_1 != '':
                  tuple_food_1 = (Time, Temp_Food_1)
                  ch.basic_publish(exchange="", routing_key=queue_name_2, body=str(tuple_food_1))
                  logging.info(f'Sent {tuple_food_1} to {queue_name_2}')
                
                if Temp_Food_2 != '':
                  tuple_food_2 = (Time, Temp_Food_2)
                  ch.basic_publish(exchange="", routing_key=queue_name_3, body=str(tuple_food_2))
                  logging.info(f'Sent {tuple_food_2} to {queue_name_3}')

        except pika.exceptions.AMQPConnectionError as e:
            logging.info(f"Error: Connection to RabbitMQ server failed: {e}")
            sys.exit(1)
        finally:
            # close the connection to the server
            conn.close()



# ===== Main ==================================================================

if __name__ == "__main__":

    # Check to see if we want to ask the user to open the website
    if SHOW_WEB == True:
        # If so, ask user to open website
        offer_website()

    # Send the file to the queue
    send_msg(HOST, QUEUE_NAME_SMOKER, QUEUE_NAME_FOOD_1, QUEUE_NAME_FOOD_2, FILE_TO_READ)