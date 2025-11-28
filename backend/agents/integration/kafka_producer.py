import json
import uuid
from typing import Any, Dict, Optional, Union, List

from kafka import KafkaProducer, errors as kafka_errors

from backend.agents.base import AgentBase
from backend.agents.registry import register_agent


def _ensure_list(value: Union[str, List[str]]) -> List[str]:
    if isinstance(value, str):
        return [value]
    return value


@register_agent("kafka_producer")
class KafkaProducerAgent(AgentBase):
    """
    Agent d'envoi de messages dans Kafka.

    Config attendue :
    {
        "bootstrap_servers": ["kafka:9092"],
        "topic": "prediction_requests",
        "key_field": "correlation_id"   # optionnel
    }
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self._producer: Optional[KafkaProducer] = None

    def validate(self):
        if "bootstrap_servers" not in self.config:
            raise ValueError("'bootstrap_servers' est obligatoire pour KafkaProducerAgent")
        if "topic" not in self.config:
            raise ValueError("'topic' est obligatoire pour KafkaProducerAgent")

    def _get_producer(self) -> KafkaProducer:
        if self._producer is None:
            servers = _ensure_list(self.config["bootstrap_servers"])
            self._producer = KafkaProducer(
                bootstrap_servers=servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
            )
        return self._producer

    def run(self, inputs: Any) -> Dict[str, Any]:
        if not isinstance(inputs, dict):
            raise ValueError("KafkaProducerAgent attend un dict en entrée.")

        # correlation_id : on génère si absent
        correlation_id = inputs.get("correlation_id")
        if not correlation_id:
            correlation_id = str(uuid.uuid4())
            inputs["correlation_id"] = correlation_id

        topic: str = self.config["topic"]
        key_field: Optional[str] = self.config.get("key_field")

        key: Optional[str] = None
        if key_field and key_field in inputs:
            key = str(inputs[key_field])

        producer = self._get_producer()

        try:
            future = producer.send(topic, value=inputs, key=key)
            record_metadata = future.get(timeout=10)
        except kafka_errors.KafkaError as e:
            raise RuntimeError(f"Erreur lors de l'envoi Kafka : {e}") from e

        return {
            "status": "sent",
            "topic": record_metadata.topic,
            "partition": record_metadata.partition,
            "offset": record_metadata.offset,
            "correlation_id": correlation_id,
        }
