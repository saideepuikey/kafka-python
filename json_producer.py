from confluent_kafka import Producer
from admin import Admin
import json
from producer import ProducerClass
import jsonschema

schema = {
    "type": "object",
    "properties": {
        "first_name": {"type": "string"},
        "last_name": {"type": "string"},
        "age": {
            "type": "integer",
            "minimum": 0
        },
    },
}

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

class JSONProducerClass(ProducerClass):
    def __init__(self, bootstrap_server, topic, schema):
        super().__init__(bootstrap_server, topic)
        self.schema = schema
        self.value_serializer = lambda v: json.dumps(v).encode('utf-8')

    def send_message(self, message):
        try:
            #validate the schema
            jsonschema.validate(message, self.schema)
            json_message = self.value_serializer(message)
            self.producer.produce(self.topic, json_message)
            print(f"Message sent")
        except Exception as e:
            print(e)

    def commit(self):
        self.producer.flush()

if __name__ == "__main__":
    bootstrap_server = "localhost:19092"
    topic = "test-topic"
    a = Admin(bootstrap_server)
    a.create_topic(topic)

    p = JSONProducerClass(bootstrap_server, topic, schema)
    
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
