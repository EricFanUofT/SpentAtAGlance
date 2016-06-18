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
        #open a file to write the historical data to
	file=open('transaction_data.json','w')

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
                
        #a factor to take into account for more transactions occuring during certain time of the year
        month_factor=[1, 1, 1.5 , 1, 1, 1.3, 1.5, 1.3, 1, 1, 1.5, 1.8]
        #a factor to take into account for more transactions occurring during the weekends
        weekend_factor=[1,1,1,1,1.5,1.5,1.5]

        #mu is the average and sigma is the standard deviation in the normal distribution for randomly generating the transaction amount
        mu = 1.5
        sigma = 1
        while True:
            #Transaction date is today, transaction time is now
            transaction_datetime=datetime.now()
            transaction_date=transaction_datetime.strftime("%y-%m-%d")
            transaction_time=transaction_datetime.strftime("%H-%M-%S")

            transaction_weekday=transaction_datetime.weekday()+1
            transaction_month=transaction_datetime.month
            
            name=names[random.randint(0, len(names)-1)]+' '+lastname_initials[random.randint(0,len(lastname_initials)-1)]+' '+str(random.randint(0,99))

            #randomly generate a transaction amount from normal distribution (mu=15 and sigma=10)
            adjusted_mu=month_factor[transaction_month-1]*weekend_factor[transaction_weekday-1]*mu

            transaction_amount=random.gauss(adjusted_mu,sigma)
            #limit the minimum transaction amount to $0.5
            if transaction_amount<0.5:
                transaction_amount=0.5
            transaction_info={"date": transaction_date,
                              "day_of_week":transaction_weekday,
                              "time": str(transaction_time),
                              "name": name,
                              "amount":transaction_amount}
            self.producer.send('transactions_message', transaction_info)

if __name__ == "__main__":

    args = sys.argv

#    ip_addr = str(args[1])

#    partition_key = str(args[2])

    prod = Producer()

    prod.produce_msgs()

    
