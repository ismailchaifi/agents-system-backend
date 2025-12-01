"""
Kafka Consumer pour traiter les demandes de prédiction.

Le consumer :
1. Écoute le topic `prediction_requests`
2. Exécute le traitement (via un agent ML ou logique)
3. Envoie la réponse dans `prediction_responses`
"""

import json
import logging
from typing import Any, Dict, List, Optional, Callable

from kafka import KafkaConsumer, KafkaProducer, errors as kafka_errors
from kafka.structs import ConsumerRecord

logger = logging.getLogger(__name__)


class KafkaConsumerProcessor:
    """
    Consumer Kafka qui traite les messages de prédiction.
    
    Config attendue :
    {
        "bootstrap_servers": ["kafka:9092"],
        "input_topic": "prediction_requests",
        "output_topic": "prediction_responses",
        "group_id": "prediction-processor",
        "auto_offset_reset": "earliest",  # ou "latest"
        "max_poll_records": 100
    }
    """

    def __init__(self, config: Dict[str, Any]):
        """Initialiser le consumer avec la configuration."""
        self.config = config
        self.validate()
        
        self._consumer: Optional[KafkaConsumer] = None
        self._producer: Optional[KafkaProducer] = None
        self._running = False
        self._process_fn: Optional[Callable] = None

    def validate(self) -> None:
        """Valider la configuration requise."""
        required_keys = ["bootstrap_servers", "input_topic", "output_topic", "group_id"]
        for key in required_keys:
            if key not in self.config:
                raise ValueError(f"'{key}' est obligatoire dans la config du consumer")

    def _get_consumer(self) -> KafkaConsumer:
        """Obtenir ou créer le consumer Kafka."""
        if self._consumer is None:
            bootstrap_servers = self.config["bootstrap_servers"]
            group_id = self.config["group_id"]
            input_topic = self.config["input_topic"]
            auto_offset_reset = self.config.get("auto_offset_reset", "earliest")
            max_poll_records = self.config.get("max_poll_records", 100)

            self._consumer = KafkaConsumer(
                input_topic,
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
                auto_offset_reset=auto_offset_reset,
                max_poll_records=max_poll_records,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                enable_auto_commit=True,
                session_timeout_ms=30000,
            )
            logger.info(
                f"Consumer créé - Topic: {input_topic}, Group: {group_id}, "
                f"Servers: {bootstrap_servers}"
            )
        return self._consumer

    def _get_producer(self) -> KafkaProducer:
        """Obtenir ou créer le producer Kafka pour envoyer les réponses."""
        if self._producer is None:
            bootstrap_servers = self.config["bootstrap_servers"]
            self._producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
            )
            logger.info(f"Producer créé - Servers: {bootstrap_servers}")
        return self._producer

    def set_process_function(self, process_fn: Callable) -> None:
        """
        Définir la fonction de traitement des messages.
        
        Args:
            process_fn: Fonction(message: dict) -> dict qui traite une demande
                       et retourne une réponse.
        """
        self._process_fn = process_fn

    def _process_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Traiter un message de demande de prédiction.
        
        Args:
            message: Message de demande reçu
            
        Returns:
            Dictionnaire contenant la réponse
        """
        if self._process_fn is None:
            raise RuntimeError(
                "Aucune fonction de traitement définie. "
                "Utilisez set_process_function() pour en définir une."
            )

        try:
            # Appeler la fonction de traitement
            result = self._process_fn(message)
            
            # S'assurer que le résultat contient correlation_id si présent dans le message
            if "correlation_id" in message and "correlation_id" not in result:
                result["correlation_id"] = message["correlation_id"]
            
            # Ajouter un statut de succès
            if "status" not in result:
                result["status"] = "success"
            
            logger.info(f"Message traité avec succès: {message.get('correlation_id')}")
            return result

        except Exception as e:
            logger.error(f"Erreur lors du traitement du message: {e}", exc_info=True)
            return {
                "correlation_id": message.get("correlation_id"),
                "status": "error",
                "error": str(e),
                "original_request": message,
            }

    def _send_response(self, response: Dict[str, Any], record: ConsumerRecord) -> None:
        """
        Envoyer la réponse dans le topic de sortie.
        
        Args:
            response: Réponse à envoyer
            record: Record original du consumer (pour récupérer les métadonnées)
        """
        output_topic = self.config["output_topic"]
        producer = self._get_producer()

        key = response.get("correlation_id")

        try:
            future = producer.send(output_topic, value=response, key=key)
            metadata = future.get(timeout=10)
            logger.info(
                f"Réponse envoyée au topic {output_topic}, "
                f"partition {metadata.partition}, offset {metadata.offset}"
            )
        except kafka_errors.KafkaError as e:
            logger.error(f"Erreur lors de l'envoi de la réponse: {e}", exc_info=True)
            raise

    def run(self) -> None:
        """
        Démarrer le consumer et traiter les messages en continu.
        
        Cette méthode boucle indéfiniment jusqu'à ce que _running soit False.
        """
        self._running = True
        consumer = self._get_consumer()
        
        logger.info("Consumer démarré. En attente de messages...")

        try:
            for record in consumer:
                if not self._running:
                    logger.info("Arrêt du consumer demandé.")
                    break

                logger.debug(f"Message reçu: {record.value}")
                
                # Traiter le message
                response = self._process_message(record.value)
                
                # Envoyer la réponse
                self._send_response(response, record)

        except KeyboardInterrupt:
            logger.info("Consumer interrompu par l'utilisateur.")
        except Exception as e:
            logger.error(f"Erreur fatale dans le consumer: {e}", exc_info=True)
            raise
        finally:
            self.stop()

    def stop(self) -> None:
        """Arrêter le consumer et nettoyer les ressources."""
        self._running = False
        
        if self._consumer is not None:
            self._consumer.close()
            self._consumer = None
            logger.info("Consumer fermé.")
        
        if self._producer is not None:
            self._producer.close()
            self._producer = None
            logger.info("Producer fermé.")

    def process_batch(self, messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Traiter un lot de messages (mode batch au lieu de streaming).
        
        Args:
            messages: Liste de messages à traiter
            
        Returns:
            Liste des réponses traitées
        """
        responses = []
        producer = self._get_producer()
        output_topic = self.config["output_topic"]

        for message in messages:
            try:
                response = self._process_message(message)
                responses.append(response)
                
                # Envoyer chaque réponse
                key = response.get("correlation_id")
                producer.send(output_topic, value=response, key=key)

            except Exception as e:
                logger.error(f"Erreur lors du traitement du batch: {e}", exc_info=True)
                responses.append({
                    "status": "error",
                    "error": str(e),
                    "original_request": message,
                })

        # Forcer la flush des messages
        producer.flush()
        return responses
