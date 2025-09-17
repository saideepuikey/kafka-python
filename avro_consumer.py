from confluent_kafka import Consumer
from confluent_kafka.schema_registry.avro import AvroDeserializer
from schema_registry_avro_client import AvroSchemaRegisterClient
from confluent_kafka.serialization import SerializationContext, MessageField

class AvroConsumerClass:
    def __init__(self, bootstrap_server, group_id, topic, schema_registry_client, schema_str):
        self.bootstrap_server = bootstrap_server
        self.group_id = group_id
        self.topic = topic
        self.consumer = Consumer({
            "bootstrap.servers": self.bootstrap_server,
            "group.id": self.group_id
            })
        self.schema_registry_client = schema_registry_client
        self.schema_str = schema_str
        self.value_serializer = AvroDeserializer(self.schema_registry_client, self.schema_str)
        
        
    def consumer_messages(self):
        self.consumer.subscribe([self.topic])
        
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Error while consuming error: {msg.error()}")
                    continue
                message = msg.value()
                deserialize_message = self.value_serializer(message, SerializationContext(self.topic, MessageField.VALUE))

                print(f"Messged consumed: {deserialize_message}, Type is: {type(deserialize_message)}")
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()


if __name__ == "__main__":
    bootstrap_server = "localhost:19092"
    group_id = "my-group-id"
    topic = "avro-schema-topic"
    schema_type = "AVRO"
    schema_url = "http://0.0.0.0:18081"

    with open("schema.avsc") as avro_file:
        avro_schema = avro_file.read()
    

    avro_schema_client = AvroSchemaRegisterClient(schema_url, topic, avro_schema, schema_type)

    schema_str = avro_schema_client.get_schema_str()

    c = AvroConsumerClass(bootstrap_server, group_id, topic, avro_schema_client.schema_reg_client, schema_str, )
    c.consumer_messages()
