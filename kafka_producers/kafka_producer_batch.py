__author__= 'EricFan'

import os
import json
import random
import sys

from datetime import date, timedelta, datetime
from kafka import KafkaProducer

class Producer(object):
    '''Kafka producer class to produce historical data for batch processing
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
        #Assume the average transaction is $10 with a standard deviation of $5
        
        #nominal total number of transactions(for all users combined) per day fluctuates with the month of the year
        num_transaction_per_day_by_month=[200, 200, 300, 200, 200, 250, 300, 250, 200, 200, 300, 350]
        #a factor to take into account for more transactions occurring during the weekends
        weekend_factors=[1,1,1,1,1.5,1.5,1.5]

        #start generating transactions from Jan 1, 2015
        transaction_date=date(2015,1,1)
        end_date=date.today()
        delta=timedelta(days=1)
        while transaction_date<=end_date:
            transaction_weekday=transaction_date.weekday()+1
            transaction_month=transaction_date.month
	    #print transaction_date.strftime("%Y-%m-%d")+' '+str(transaction_month)+' '+str(transaction_weekday)
            
            #determine the number of transactions (for all users combined) for that day
            num_transactions=int(num_transaction_per_day_by_month[transaction_month-1]*weekend_factors[transaction_weekday-1])

            #distribute the transactions from 8:00:00 am to 8:00:00pm (43200 sec)
            delay=43200/num_transactions

            for i in range(1, num_transactions+1):
                second=int(((i*delay)%60))
                minute=int(((i*delay)/60)%60)
                hour=int(8+i*delay/3600)
                transaction_time='{0:02d}'.format(hour)+'-'+'{0:02d}'.format(minute)+'-'+'{0:02d}'.format(second)		    
	        #randomly generate a card holder name
                name=names[random.randint(0, len(names)-1)]+' '+lastname_initials[random.randint(0,len(lastname_initials)-1)]
                #randomly generate a transaction amount from normal distribution (mu=15 and sigma=10)
                transaction_amount=random.gauss(15,10)
 	        #limit the minimum transaction amount to $1
                if transaction_amount<1.0:
                    transaction_amount=1.0
                transaction_info={"date": transaction_date.strftime("%y-%m-%d"),
                                  "day_of_week":transaction_weekday,
                                  "time": str(transaction_time),
                                  "name": name,
                                  "amount":transaction_amount}
                #write transaction_info to file
                file.write(str(transaction_info)+'\n')                 
                print str(transaction_info)
            transaction_date=transaction_date+delta 
        file.close()

if __name__ == "__main__":

    args = sys.argv

#    ip_addr = str(args[1])

#    partition_key = str(args[2])

    prod = Producer()

    prod.produce_msgs()

    
