from backend.agents.integration.kafka_producer import KafkaProducerAgent

# On crée un faux "producer" qui imite KafkaProducer
class DummyFuture:
    def get(self, timeout=None):
        # On simule un metadata de record Kafka
        return type(
            "RecordMetadata",
            (),
            {"topic": "prediction_requests", "partition": 0, "offset": 42},
        )()


class DummyProducer:
    def __init__(self, *args, **kwargs):
        pass

    def send(self, topic, value=None, key=None):
        print(f"[DummyProducer] send() appelé avec topic={topic}, key={key}, value={value}")
        return DummyFuture()


# On dérive l'agent pour remplacer _get_producer par notre DummyProducer
class TestKafkaProducerAgent(KafkaProducerAgent):
    def _get_producer(self):
        return DummyProducer()


def main():
    config = {
        "bootstrap_servers": ["kafka:9092"],  # valeur fictive ici
        "topic": "prediction_requests",
        "key_field": "correlation_id",
    }

    agent = TestKafkaProducerAgent(config=config)
    agent.validate()

    inputs = {
        "supplier": "SuperMarché Alpha",
        "invoice_date": "2023-01-31",
        "ht": 100.0,
        "tva": 20.0,
        "ttc": 120.0,
        # on peut omettre correlation_id pour vérifier qu'il est généré
    }

    result = agent.run(inputs)
    print("Résultat KafkaProducerAgent :", result)


if __name__ == "__main__":
    main()
