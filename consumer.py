from confluent_kafka import Consumer

class ConsumerClass:
    def __init__(self, bootstrap_server, group_id, topic):
        self.bootstrap_server = bootstrap_server
        self.group_id = group_id
        self.consumer = Consumer({
            "bootstrap.servers": self.bootstrap_server,
            "group.id": self.group_id
            })
        self.topic = topic
        
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
                print(f"Messged consumed: {msg.value()}")
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()


if __name__ == "__main__":
    bootstrap_server = "localhost:19092"
    group_id = "my-group-id"
    topic = "test-topic"

    c = ConsumerClass(bootstrap_server, group_id, topic)
    c.consumer_messages()