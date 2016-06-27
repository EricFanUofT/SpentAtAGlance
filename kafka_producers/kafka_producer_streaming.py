__author__= 'EricFan'

import os
import json
import random
import sys

from datetime import date, timedelta, datetime
from kafka import KafkaProducer

class Producer(object):
    '''Kafka producer class to produce a stream of data for stream processing
       data will be in json formatted strings
    '''

    def __init__(self):
	'''Initialize Producer with Kafka broker address (so that it can be consumed by camus if necessary'''
        self.producer = KafkaProducer(bootstrap_servers=["52.40.145.60:9092"],
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                                      acks=0,
                                      linger_ms=500)

    def produce_msgs(self):
        #100 unique cardholder first names
        names = ['Noah', 'Liam', 'Mason', 'Jacob', 'William', 'Ethan', 'Michael', 'Alexander', 'James', 'Daniel',
                  'Elijah', 'Benjamin', 'Logan', 'Aiden', 'Jayden', 'Matthew', 'Jackson', 'David', 'Lucas', 'Joseph',
                  'Anthony', 'Andrew', 'Samuel', 'Gabriel', 'Joshua', 'John', 'Carter', 'Luke', 'Dylan', 'Christopher',
                  'Isaac', 'Oliver', 'Henry', 'Sebastian', 'Caleb', 'Owen', 'Ryan', 'Nathan', 'Wyatt', 'Hunter',
                  'Jack', 'Christian', 'Landon', 'Jonathan', 'Levi', 'Jaxon', 'Julian', 'Isaiah', 'Eli', 'Aaron',
                  'Sarah', 'Nora', 'Skylar', 'Riley', 'Sadie', 'Aaliyah', 'Claire', 'Gabriella', 'Penelope', 'Camila',
                  'Arianna', 'Savannah', 'Allison', 'Ariana', 'Audrey', 'Leah', 'Anna', 'Samantha', 'Zoe', 'Aria',
                  'Scarlett', 'Layla', 'Hannah', 'Lily', 'Brooklyn', 'Lillian', 'Addison', 'Natalie', 'Zoey', 'Grace',
                  'Aubrey', 'Victoria', 'Chloe', 'Ella', 'Evelyn', 'Amelia', 'Elizabeth', 'Avery', 'Sofia', 'Harper',
                  'Charlotte', 'Madison', 'Abigail', 'Emily', 'Mia', 'Ava', 'Isabella', 'Sophia', 'Olivia', 'Emma']
        #26 last names initials
        lastname_initials=['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z']

        #include 20 types of credit card purchase
        transaction_types=['cd/dvd', 'household item','in-app purchase','office supply', 'toy', 'clothing','restaurant', 'entertainment','grocery','sports', 'baby/toddler','gift','beauty/healthcare','fuel/gas','electronics', 'auto/tires', 'furniture','jewelry', 'computer', 'appliance']
        #the average price corresponding to each type of transaction
        mu={'cd/dvd':7.5, 'household item':10,'in-app purchase':10,'office supply':15, 'toy':20, 'clothing':20,'restaurant':20, 'entertainment':25,'grocery':30,'sports':30,  'baby/toddler':30,'gift':30,'beauty/healthcare':50,'fuel/gas':50,'electronics':300, 'auto/tires':500, 'furniture':500,'jewelry':750, 'computer':800, 'appliance': 1000}

        sigma={'cd/dvd':2, 'household item':3,'in-app purchase':3,'office supply':4, 'toy':5, 'clothing':6,'restaurant':6, 'entertainment':6.5,'grocery':8, 'sports':10, 'baby/toddler':8,'gift':10,'beauty/healthcare':15,'fuel/gas':5,'electronics':90, 'auto/tires':150, 'furniture':150,'jewelry':200, 'computer':150, 'appliance': 200}

        #nominal total number of transactions(for all users combined) per day fluctuates with the month of the year
        num_transaction_per_day_by_month=[400000, 400000, 650000, 500000, 500000, 700000, 750000, 500000, 500000, 600000, 750000, 900000]
        #a factor to take into account for more transactions occurring during the weekends
        weekend_factors=[0.9,0.9,1,0.9,1.5,1.4,1.5]

        while True:
            #Transaction date is today, transaction time is now
            transaction_datetime=datetime.now()
            transaction_date=transaction_datetime.strftime("%Y-%m-%d")
            transaction_time=transaction_datetime.strftime("%H-%M-%S")

            transaction_weekday=transaction_datetime.weekday()+1
            transaction_month=transaction_datetime.month

            #randomly generate a card holder name
            name=names[random.randint(0, len(names)-1)]+' '+lastname_initials[random.randint(0,len(lastname_initials)-1)]+'{0:03d}'.format(random.randint(0,999))
            #randomly generate a transaction type with different chances of appearing
            num=random.randint(1,100)
            if num<=50:
                type_index=random.randint(0,7)
            elif num<=85:
                type_index=random.randint(8,13)
            elif num<=97:
                type_index=random.randint(14,16)
            else:
                type_index=random.randint(17,19)
 
            transaction_type=transaction_types[type_index]
            #randomly generate the transaction amount using a normal distribution with (mu,sigma) corresponding to the particular transaction type
            transaction_amount=random.gauss(mu[transaction_type],sigma[transaction_type])
            #limit the minimum transaction amount to $1
            if transaction_amount<1.0:
                transaction_amount=1.0
           
            transaction_info={"date": transaction_date,
                              "day_of_week":transaction_weekday,
                              "time": str(transaction_time),
                              "name": name,
                              "trans_type": transaction_type,
                              "amount":transaction_amount}
            #send the message to a Kafka topic
            self.producer.send('transactions_message', transaction_info)

if __name__ == "__main__":

    args = sys.argv

#    ip_addr = str(args[1])

#    partition_key = str(args[2])

    prod = Producer()

    prod.produce_msgs()

    
