# streaming-05-smart-smoker
> Example of streaming data from one publisher (producer) to multiple consumers via a queue.

### Author: Solomon Stevens
### Date: May 31, 2024

## Prerequisites
* RabbitMQ Installed and running on your machine
* Pika installed and running on Python

## How it works
* Timestamps and temperatures are read in from `smoker-temps.csv` by `temperature_producer.py`
* Temperatures are separated by channel (column) and sent to respective queues
* Each consumer script reads in the contents of a unique queue
* -> A different queue displays the temperature of a different object (smoker, different foods)
* Each consumer checks if there is an issue with the registered temperatures
* -> There is a 15 degree drop in the smoker over the 'span' of 2.5 minutes
* -> There is not even 1 degree of change in a food for at least 10 minutes
* -> The consumer sends a "flag" message letting the user know

## How to run it yourself
1. Start by running `temperature_producer.py`.  This will create and begin to fill the queues.
> python temperature_producer.py
- NOTE: Run the producer first. It deletes any queues that share the name of the ones it wants to create
- -> Running consumers first will close their connection to any pre-existing queues
2. Run each consumer script:
> python temperature_consumer_food_1.py
> python temperature_consumer_food_2.py
> python temperature_consumer_smoker.py
3. Watch the magic happen


## Screenshots
![image1](images/Smoker(1).png)
![image2](images/Smoker(2).png)

