# consumer.py - Kafka Consumer to store messages in SQLite

from kafka import KafkaConsumer
import sqlite3

# Define SQLite database name
database_name = "faker_data.db"

# SQL statement to insert data into the "faker" table
faker_insert = (
    '''
        INSERT INTO faker (id, name, address, telephone, birthdate, city, email, country, job, timezone)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''
)

# Establish SQLite database connection
conn = sqlite3.connect(database_name)
cursor = conn.cursor()

# Initialize Kafka Consumer to listen to the 'faker_data' topic
consumer = KafkaConsumer('faker_data',
                         bootstrap_servers='localhost:9092',  # Kafka broker
                         auto_offset_reset='earliest',  # Start reading from the beginning of the topic
                         value_deserializer=lambda v: v.decode('utf-8'),  # Convert byte messages to strings
                         api_version=(0, 10, 1)  # Kafka API version
)

# Process messages received from Kafka topic
for message in consumer:
    data = message.value
    # Parse received data by splitting the fields using ' - '
    cursor.execute(faker_insert, (
        data.split(' - ')[0],  # id
        data.split(' - ')[1],  # name
        data.split(' - ')[2].replace('\n',' '),  # address
        data.split(' - ')[3],  # telephone
        data.split(' - ')[4],  # birthdate
        data.split(' - ')[5],  # city
        data.split(' - ')[6],  # email
        data.split(' - ')[7],  # country
        data.split(' - ')[8],  # job
        data.split(' - ')[9]   # timezone
    ))
    conn.commit()  # Save changes to the database
    print(f"Inserted data for the faker SQL table {data}")

# Close the database connection
cursor.close()
