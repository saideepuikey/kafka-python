from admin import Admin
from producer import ProducerClass
from schema_registry_avro_client import AvroSchemaRegisterClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

class User:
    def __init__(self, first_name, last_name, age):
        self.first_name = first_name
        self.last_name = last_name
        self.age = age
    
def user_to_dict(user):
    return dict(
            first_name = user.first_name,
            last_name = user.last_name,
            age = user.age
    )

class AvroProducerClass(ProducerClass):
    def __init__(self, bootstrap_server, topic, schema_registry_client, schema_str):
        super().__init__(bootstrap_server, topic)
        self.schema_registry_client = schema_registry_client
        self.schema_str = schema_str
        self.value_serializer = AvroSerializer(schema_registry_client, schema_str)

    def send_message(self, message):
        try:
            #validate the schema
            avro_byte_message = self.value_serializer(message, SerializationContext(self.topic, MessageField.VALUE))
            self.producer.produce(self.topic, avro_byte_message)
            print(f"Message sent: {avro_byte_message}")
        except Exception as e:
            print(e)

    def commit(self):
        self.producer.flush()

if __name__ == "__main__":
    bootstrap_server = "localhost:19092"
    topic = "avro-schema-topic"
    schema_url = "http://0.0.0.0:18081"
    schema_type = "AVRO"
    
    # Create topic
    a = Admin(bootstrap_server)
    a.create_topic(topic)
    
    # Register the schema
    with open("schema.avsc") as avro_schema:
        avro_schema_str = avro_schema.read()

    avro_schema_client = AvroSchemaRegisterClient(schema_url, topic, avro_schema_str, schema_type)
    avro_schema_client.register_schema()

    # Get schema from the Schema Registry
    schema_str = avro_schema_client.get_schema_str()

    # Produce the message
    p = AvroProducerClass(bootstrap_server, topic, avro_schema_client.schema_reg_client, schema_str)
    
    try:
        while True:
            first_name = input("Enter your first name: ")
            last_name = input("Enter your last name: ")
            age = int(input("Enter your first age: "))
            user = User(first_name, last_name, age)
            p.send_message(user_to_dict(user))
    except KeyboardInterrupt as e:
        pass

    p.commit()
