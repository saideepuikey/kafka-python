from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.error import SchemaRegistryError

class AvroSchemaRegisterClient:
    def __init__(self, schema_url, subject, schema_str, schema_type):
        self.schema_url = schema_url
        self.subject = subject
        self.schema_str = schema_str
        self.schema_type = schema_type
        self.schema_reg_client = SchemaRegistryClient({"url" : self.schema_url})

    def check_schema_exists(self):
        try:
            self.schema_reg_client.get_latest_version(self.subject)
            return True
        except:
            return False

    def register_schema(self):
        if not self.check_schema_exists():
            try:
                schema = Schema(self.schema_str, self.schema_type)
                self.schema_reg_client.register_schema(self.subject, schema)
                print("Schema register successful!")
            except SchemaRegistryError as e:
                print(e)

if __name__ == "__main__":
    schema_url = "http://0.0.0.0:18081"
    subject = "avro-msg-topic"
    schema_type = "AVRO"

    with open("schema.avsc") as avro_schema:
        schema_str = avro_schema.read()

    sc = AvroSchemaRegisterClient(schema_url, subject, schema_str, schema_type)
    sc.register_schema()
