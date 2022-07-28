from kafka import KafkaProducer
from datetime import datetime
import time
import random
import pandas as pd 
import numpy as np

# load data: 
data_predict = pd.read_csv("C:\kafka-demo\data\stream_final.csv")
# pip install kafka-python

KAFKA_TOPIC_NAME_CONS = "demo22"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")

    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                                       value_serializer=lambda x: x.encode('utf-8'))

    # product_name_list = ["Laptop", "Desktop Computer", "Mobile Phone", "Wrist Band", "Wrist Watch", "LAN Cable",
    #                      "HDMI Cable", "TV", "TV Stand", "Text Books", "External Hard Drive", "Pen Drive", "Online Course"]

    # order_card_type_list = ["Visa", "MasterCard", "Maestro"]

    # country_name_city_name_list = ["Sydney,Australia", "Florida,United States", "New York City,United States",
    #                                "Paris,France", "Colombo,Sri Lanka", "Dhaka,Bangladesh", "Islamabad,Pakistan",
    #                                "Beijing,China", "Rome,Italy", "Berlin,Germany", "Ottawa,Canada",
    #                                "London,United Kingdom", "Jerusalem,Israel", "Bangkok,Thailand",
    #                                "Chennai,India", "Bangalore,India", "Mumbai,India", "Pune,India",
    #                                "New Delhi,Inida", "Hyderabad,India", "Kolkata,India", "Singapore,Singapore"]

    # ecommerce_website_name_list = ["www.datamaking.com", "www.amazon.com", "www.flipkart.com", "www.snapdeal.com", "www.ebay.com"]

    message_list = []
    message = None
    
    massage_total = []
    for i in range(1,100000):
        message_fields_value_list = []
        # message_fields_value_list.append(str(data_predict.iloc[i, 11]))
        message_fields_value_list.append(str(data_predict.iloc[i, 0]))
        message_fields_value_list.append(str(data_predict.iloc[i, 1]))
        message_fields_value_list.append(str(data_predict.iloc[i, 2]))
        message_fields_value_list.append(str(data_predict.iloc[i, 3]))
        message_fields_value_list.append(str(data_predict.iloc[i, 4]))
        message_fields_value_list.append(str(data_predict.iloc[i, 5]))
        message_fields_value_list.append(str(data_predict.iloc[i, 6]))
        message_fields_value_list.append(str(data_predict.iloc[i, 7]))
        message_fields_value_list.append(str(data_predict.iloc[i, 8]))
        message_fields_value_list.append(str(data_predict.iloc[i, 9]))
        message_fields_value_list.append(str(data_predict.iloc[i, 10]))
        # message_fields_value_list.append(str(data_predict.iloc[i, 11]))
        message = ",".join(message_fields_value_list)
        print("Message Type: ", type(message))
        print("Message: ", message)
        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, message)
        if i % 10 == 0:
            time.sleep(15)
    # for i in range(2000):
    #     i = i + 1
    #     message_fields_value_list = []
    #     print("Preparing message: " + str(i))
    #     event_datetime = datetime.now()

    #     message_fields_value_list.append(str(i))
    #     message_fields_value_list.append(random.choice(product_name_list))
    #     message_fields_value_list.append(random.choice(order_card_type_list))
    #     message_fields_value_list.append(str(round(random.uniform(5.5, 555.5), 2)))
    #     message_fields_value_list.append(event_datetime.strftime("%Y-%m-%d %H:%M:%S"))
    #     country_name = None
    #     city_name = None
    #     country_name_city_name = None
    #     country_name_city_name = random.choice(country_name_city_name_list)
    #     country_name = country_name_city_name.split(",")[1]
    #     city_name = country_name_city_name.split(",")[0]
    #     message_fields_value_list.append(country_name)
    #     message_fields_value_list.append(city_name)
    #     message_fields_value_list.append(random.choice(ecommerce_website_name_list))

    #     # order_id,order_product_name,order_card_type,order_amount,order_datetime,order_country_name,order_city_name,order_ecommerce_website_name
    #     message = ",".join(message_fields_value_list)
    #     print("Message Type: ", type(message))
    #     print("Message: ", message)
    #     #message_list.append(message)
        
        
        
    #     kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, message)
    #     time.sleep(1)

    # print(message_list)

    print("Kafka Producer Application Completed. ")
    