from backend.agents.integration.kafka_response_wait import KafkaResponseWaitAgent


# Faux consumer Kafka qui retourne UNE SEULE réponse avec le bon correlation_id
class DummyConsumer:
    def __init__(self):
        self._called = False

    def poll(self, timeout_ms=None):
        # Première fois → on renvoie un message
        if not self._called:
            self._called = True

            class Msg:
                # valeur déjà désérialisée en dict dans notre agent
                value = {
                    "correlation_id": "abc-123",
                    "result": {
                        "risk_score": 0.87,
                        "decision": "REVIEW",
                    },
                }

            # poll() retourne un dict {TopicPartition: [Msg, ...]}
            return {None: [Msg()]}
        # Après → plus rien
        return {}


class TestKafkaResponseWaitAgent(KafkaResponseWaitAgent):
    def _get_consumer(self):
        # On ne crée pas de vrai KafkaConsumer, on retourne juste notre DummyConsumer
        return DummyConsumer()


def main():
    config = {
        "bootstrap_servers": ["kafka:9092"],  # fictif
        "topic": "prediction_responses",
        "group_id": "agents-system-test",
        "timeout_seconds": 5,
    }

    agent = TestKafkaResponseWaitAgent(config=config)
    agent.validate()

    inputs = {
        "correlation_id": "abc-123"
    }

    result = agent.run(inputs)
    print("Résultat KafkaResponseWaitAgent :", result)


if __name__ == "__main__":
    main()
