from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.error import SchemaRegistryError

class AvroSchemaRegisterClient:
    def __init__(self, schema_url, subject, schema_str, schema_type):
        self.schema_url = schema_url
        self.subject = subject
        self.schema_str = schema_str
        self.schema_type = schema_type
        self.schema_reg_client = SchemaRegistryClient({"url" : self.schema_url})

    # def schema_exist_in_registry(self):
    def get_schema_version(self):
        try:
            schema_version = self.schema_reg_client.get_latest_version(self.subject)
            return schema_version.schema_id
        except SchemaRegistryError as e:
            print(e)

    def get_schema_str(self):
        try:
            # schema_id = self.schema_exist_in_registry()
            schema_id = self.get_schema_version()
            schema = self.schema_reg_client.get_schema(schema_id)
            return schema.schema_str
        except SchemaRegistryError as e:
            print(e)

    def register_schema(self):
        # if not self.schema_exist_in_registry():
        if not self.get_schema_version():
            try:
                schema = Schema(self.schema_str, self.schema_type)
                self.schema_reg_client.register_schema(self.subject, schema)
                print("Schema registered successfully!")
            except SchemaRegistryError as e:
                print(e)
        else:
            print("Schema already registered!")

if __name__ == "__main__":
    schema_url = "http://0.0.0.0:18081"
    subject = "avro-schema-topic"
    schema_type = "AVRO"

    with open("schema.avsc") as avro_schema:
        schema_str = avro_schema.read()

    sc = AvroSchemaRegisterClient(schema_url, subject, schema_str, schema_type)
    sc.register_schema()
