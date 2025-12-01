from typing import List, Union
import fastapi
from pydantic import BaseModel, Field, field_validator
from pyparsing import Any, Dict, Optional
from datetime import datetime

class PredictionRequest(BaseModel):
    """
    Schéma de requête de prédiction.
    
    Représente une demande de prédiction envoyée au microservice.
    """
    
    correlation_id: Optional[str] = Field(
        default=None,
        description="ID de corrélation unique pour tracker la demande",
        example="550e8400-e29b-41d4-a716-446655440000"
    )
    
    model_name: str = Field(
        ...,
        description="Nom du modèle à utiliser pour la prédiction",
        example="gpt-3.5-turbo",
        min_length=1
    )
    
    data: Dict[str, Any] = Field(
        ...,
        description="Données d'entrée pour la prédiction",
        example={
            "features": [1.0, 2.0, 3.0],
            "text": "exemple de texte"
        }
    )
    
    parameters: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Paramètres optionnels du modèle (temperature, max_tokens, etc.)",
        example={
            "temperature": 0.7,
            "max_tokens": 500
        }
    )
    
    timestamp: Optional[datetime] = Field(
        default_factory=datetime.utcnow,
        description="Timestamp de la requête"
    )
    
    metadata: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Métadonnées additionnelles",
        example={
            "user_id": "user123",
            "session_id": "session456"
        }
    )
    
    @field_validator('model_name')
    def validate_model_name(cls, v):
        """Valider que le nom du modèle n'est pas vide."""
        if not v.strip():
            raise ValueError("model_name ne peut pas être vide")
        return v.strip()
    
    @field_validator('data')
    def validate_data(cls, v):
        """Valider que les données d'entrée ne sont pas vides."""
        if not v:
            raise ValueError("data ne peut pas être vide")
        return v
    
    class Config:
        schema_extra = {
            "example": {
                "correlation_id": "550e8400-e29b-41d4-a716-446655440000",
                "model_name": "gpt-3.5-turbo",
                "data": {
                    "features": [1.0, 2.0, 3.0],
                    "text": "Résumez ce texte"
                },
                "parameters": {
                    "temperature": 0.7,
                    "max_tokens": 500
                },
                "metadata": {
                    "user_id": "user123"
                }
            }
        }
    
class PredictionResponse(BaseModel):
    """
    Schéma de réponse de prédiction.
    
    Représente la réponse retournée par le microservice.
    """
    
    correlation_id: str = Field(
        ...,
        description="ID de corrélation pour tracker la demande",
        example="550e8400-e29b-41d4-a716-446655440000"
    )
    
    status: str = Field(
        ...,
        description="Statut de la prédiction",
        example="success",
        regex="^(success|error|pending)$"
    )
    
    prediction: Optional[Union[str, float, int, Dict[str, Any], List[Any]]] = Field(
        default=None,
        description="Résultat de la prédiction",
        example="Ceci est un résumé généré"
    )
    
    model_name: str = Field(
        ...,
        description="Nom du modèle utilisé",
        example="gpt-3.5-turbo"
    )
    
    processing_time_ms: float = Field(
        ...,
        description="Temps de traitement en millisecondes",
        example=250.5
    )
    
    timestamp: datetime = Field(
        default_factory=datetime.utcnow,
        description="Timestamp de la réponse"
    )
    
    error: Optional[str] = Field(
        default=None,
        description="Message d'erreur si status='error'",
        example="Model not found"
    )
    
    metadata: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Métadonnées additionnelles",
        example={
            "tokens_used": 150,
            "model_version": "1.0"
        }
    )
    
    class Config:
        schema_extra = {
            "example": {
                "correlation_id": "550e8400-e29b-41d4-a716-446655440000",
                "status": "success",
                "prediction": "Ceci est un résumé généré par le modèle",
                "model_name": "gpt-3.5-turbo",
                "processing_time_ms": 250.5,
                "timestamp": "2025-12-01T10:30:00",
                "metadata": {
                    "tokens_used": 150,
                    "model_version": "1.0"
                }
            }
        }


class ErrorResponse(BaseModel):
    """Schéma pour les réponses d'erreur."""
    
    status: str = Field(default="error", description="Statut d'erreur")
    error: str = Field(..., description="Message d'erreur détaillé")
    correlation_id: Optional[str] = Field(
        default=None,
        description="ID de corrélation si disponible"
    )
    timestamp: datetime = Field(
        default_factory=datetime.utcnow,
        description="Timestamp de l'erreur"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "status": "error",
                "error": "Invalid model name",
                "correlation_id": "550e8400-e29b-41d4-a716-446655440000",
                "timestamp": "2025-12-01T10:30:00"
            }
        }


router = fastapi.APIRouter()


@router.post(
    "/predict",
    response_model=Union[PredictionResponse, ErrorResponse],
    summary="Endpoint de prédiction",
    description="Recevoir une demande de prédiction et retourner le résultat.",
)
async def predict(request: PredictionRequest) -> Union[PredictionResponse, ErrorResponse]:
    """
    Endpoint pour recevoir une demande de prédiction et retourner le résultat.
    
    Logique mock:
    - Accepte les données: supplier, ht (hors taxe), tva
    - Calcule: ttc = ht + tva
    - Retourne: risk_score = ttc / 1000, prediction (risque basé sur le fournisseur)
    
    Args:
        request: Objet PredictionRequest contenant les détails de la demande.
        
    Returns:
        Objet PredictionResponse avec le résultat ou ErrorResponse en cas d'erreur.
    """
    import time
    start_time = time.time()
    
    try:
        # Valider les données requises
        data = request.data
        
        if "supplier" not in data:
            raise ValueError("'supplier' est requis dans les données")
        if "ht" not in data:
            raise ValueError("'ht' (hors taxe) est requis dans les données")
        if "tva" not in data:
            raise ValueError("'tva' est requise dans les données")
        
        supplier = str(data["supplier"]).strip()
        try:
            ht = float(data["ht"])
            tva = float(data["tva"])
        except (ValueError, TypeError) as e:
            raise ValueError(f"'ht' et 'tva' doivent être des nombres: {e}")
        
        # Logique mock de prédiction
        # Calculer TTC (Total Toutes Charges)
        ttc = ht + tva
        
        # Calculer le risk_score
        risk_score = ttc / 1000
        
        # Générer une prédiction basée sur le fournisseur et le montant
        if risk_score > 10:
            prediction = "HIGH_RISK"
            risk_level = "Élevé"
        elif risk_score > 5:
            prediction = "MEDIUM_RISK"
            risk_level = "Moyen"
        else:
            prediction = "LOW_RISK"
            risk_level = "Faible"
        
        # Ajouter une logique basée sur le fournisseur (mock)
        supplier_risk_factors = {
            "supplier_a": 0.1,
            "supplier_b": 0.2,
            "supplier_c": 0.3,
        }
        
        supplier_factor = supplier_risk_factors.get(supplier.lower(), 0.15)
        adjusted_risk_score = risk_score * (1 + supplier_factor)
        
        processing_time = (time.time() - start_time) * 1000  # en ms
        
        prediction_result = {
            "risk_score": round(risk_score, 2),
            "adjusted_risk_score": round(adjusted_risk_score, 2),
            "risk_level": risk_level,
            "prediction": prediction,
            "supplier": supplier,
            "ttc": round(ttc, 2),
            "ht": round(ht, 2),
            "tva": round(tva, 2),
            "supplier_factor": round(supplier_factor, 2),
        }
        
        return PredictionResponse(
            correlation_id=request.correlation_id or "unknown",
            status="success",
            prediction=prediction_result,
            model_name=request.model_name,
            processing_time_ms=processing_time,
            timestamp=datetime.utcnow(),
            metadata={
                "calculation_method": "ttc / 1000",
                "supplier_factors_applied": True,
                "ttc_value": ttc,
            }
        )
    
    except Exception as e:
        processing_time = (time.time() - start_time) * 1000  # en ms
        return ErrorResponse(
            status="error",
            error=str(e),
            correlation_id=request.correlation_id or "unknown",
            timestamp=datetime.utcnow()
        )