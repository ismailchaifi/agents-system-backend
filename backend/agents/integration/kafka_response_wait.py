import json
import time
from typing import Any, Dict, Optional, Union, List

from kafka import KafkaConsumer, errors as kafka_errors

from backend.agents.base import AgentBase
from backend.agents.registry import register_agent


def _ensure_list(value: Union[str, List[str]]) -> List[str]:
    if isinstance(value, str):
        return [value]
    return value


@register_agent("kafka_response_wait")
class KafkaResponseWaitAgent(AgentBase):
    """
    Agent qui attend une réponse dans un topic Kafka en filtrant par correlation_id.

    Config attendue :
    {
        "bootstrap_servers": ["kafka:9092"],
        "topic": "prediction_responses",
        "group_id": "agents-system-consumer",
        "timeout_seconds": 30
    }

    Entrée attendue (inputs) :
    {
        "correlation_id": "uuid-xxx",
        ...
    }
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self._consumer: Optional[KafkaConsumer] = None

    def validate(self):
        if "bootstrap_servers" not in self.config:
            raise ValueError("'bootstrap_servers' est obligatoire pour KafkaResponseWaitAgent")
        if "topic" not in self.config:
            raise ValueError("'topic' est obligatoire pour KafkaResponseWaitAgent")
        if "group_id" not in self.config:
            raise ValueError("'group_id' est obligatoire pour KafkaResponseWaitAgent")

    def _get_consumer(self) -> KafkaConsumer:
        if self._consumer is None:
            servers = _ensure_list(self.config["bootstrap_servers"])
            topic = self.config["topic"]
            group_id = self.config["group_id"]

            self._consumer = KafkaConsumer(
                topic,
                bootstrap_servers=servers,
                group_id=group_id,
                auto_offset_reset="latest",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                enable_auto_commit=True,
            )

        return self._consumer

    def run(self, inputs: Any) -> Dict[str, Any]:
        if not isinstance(inputs, dict):
            raise ValueError("KafkaResponseWaitAgent attend un dict en entrée.")
        if "correlation_id" not in inputs:
            raise ValueError("KafkaResponseWaitAgent nécessite 'correlation_id' dans inputs.")

        expected_cid = str(inputs["correlation_id"])
        timeout_seconds: int = int(self.config.get("timeout_seconds", 30))

        consumer = self._get_consumer()

        start = time.time()
        try:
            while True:
                if time.time() - start > timeout_seconds:
                    raise TimeoutError(
                        f"Aucune réponse trouvée pour correlation_id={expected_cid} dans le délai imparti."
                    )

                msg_pack = consumer.poll(timeout_ms=1000)
                # msg_pack est un dict {TopicPartition: [Message, ...]}
                for _, messages in msg_pack.items():
                    for msg in messages:
                        value = msg.value  # déjà désérialisé en dict
                        cid = str(value.get("correlation_id", ""))

                        if cid == expected_cid:
                            # On a trouvé la bonne réponse
                            return value

        except kafka_errors.KafkaError as e:
            raise RuntimeError(f"Erreur Kafka dans KafkaResponseWaitAgent : {e}") from e
