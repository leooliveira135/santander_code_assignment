# producer.py - Kafka Producer to generate and send fake data

from kafka import KafkaProducer
from faker import Faker

# Initialize Faker instance for generating fake data
faker = Faker()

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",  # Kafka broker
    api_version=(0, 10, 1),  # Kafka API version
    retries=5,  # Retry sending messages up to 5 times
    request_timeout_ms=60000,  # Maximum request timeout
    linger_ms=100,  # Delay before sending batch of messages
    batch_size=16384,  # Batch size for messages
    max_block_ms=60000  # Max blocking time
)

# Generate and send 1000 fake records
for _ in range(1000):
    id = faker.uuid4()
    name = faker.name()
    address = faker.address()
    telephone = faker.phone_number()
    birthdate = faker.date_of_birth(minimum_age=18, maximum_age=100)
    city = faker.city()
    email = faker.email()
    country = faker.country()
    job = faker.job()
    timezone = faker.timezone()
    
    # Create a formatted string with all the generated fields
    final_data = " - ".join([id, name, address, telephone, str(birthdate), city, email, country, job, timezone])
    
    try:
        # Send the generated data to Kafka topic 'faker_data'
        topic_data = producer.send(topic="faker_data", value=final_data.encode('utf-8'))
        result = topic_data.get(timeout=10)  # Wait for Kafka acknowledgment
        print("Message sent successfully: ", result)
    except Exception as e:
        print("Error during the message sent to the producer topic: ", e)

# Close the Kafka producer connection
producer.close()
