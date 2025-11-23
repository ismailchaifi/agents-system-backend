from datetime import datetime
from typing import Any, Dict, List, Optional

from backend.agents.base import AgentBase
from backend.agents.registry import register_agent


@register_agent("formatter")
class FormatterAgent(AgentBase):
    """
    Agent qui normalise les formats :
    - Dates (ex : 31/01/2023 -> 2023-01-31)
    - Nombres (ex : "100,5" -> "100.50")
    - Nettoyage des chaînes (strip)
    """

    def validate(self):
        # Vérifie que la config est cohérente
        cfg = self.config

        # date_fields : liste optionnelle
        if "date_fields" in cfg and not isinstance(cfg["date_fields"], list):
            raise ValueError("date_fields doit être une liste de champs (ou être absent).")

        # input_date_formats : liste optionnelle
        if "input_date_formats" in cfg and not isinstance(cfg["input_date_formats"], list):
            raise ValueError("input_date_formats doit être une liste de formats de date (ou être absent).")

        # numeric_fields : liste optionnelle
        if "numeric_fields" in cfg and not isinstance(cfg["numeric_fields"], list):
            raise ValueError("numeric_fields doit être une liste de champs (ou être absent).")

        # decimals : entier optionnel
        if "decimals" in cfg and not isinstance(cfg["decimals"], int):
            raise ValueError("decimals doit être un entier.")

    # ----------- Helpers internes -----------

    def _strip_and_empty_to_none(self, value: Any) -> Any:
        """Nettoie les chaînes et convertit les vides en None si demandé."""
        if isinstance(value, str):
            if self.config.get("strip_strings", True):
                value = value.strip()
            if self.config.get("empty_to_none", True) and value == "":
                return None
        return value

    def _format_date(self, value: Any, field_name: str) -> Any:
        """Formate un champ date si configuré."""
        date_fields: List[str] = self.config.get("date_fields", [])
        if field_name not in date_fields:
            return value

        if value is None:
            return None

        if not isinstance(value, str):
            return value  # on ne force pas

        input_formats: List[str] = self.config.get(
            "input_date_formats",
            ["%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"],  # formats courants par défaut
        )
        output_format: str = self.config.get("output_date_format", "%Y-%m-%d")

        for fmt in input_formats:
            try:
                dt = datetime.strptime(value, fmt)
                return dt.strftime(output_format)
            except ValueError:
                continue

        # Si aucun format ne marche, on laisse la valeur telle quelle
        return value

    def _format_number(self, value: Any, field_name: str) -> Any:
        """Formate un champ numérique si configuré."""
        numeric_fields: List[str] = self.config.get("numeric_fields", [])
        if field_name not in numeric_fields:
            return value

        if value is None:
            return None

        # Convertir en float
        if isinstance(value, str):
            # remplace virgule par point si besoin
            v = value.replace(",", ".")
        else:
            v = value

        try:
            num = float(v)
        except (ValueError, TypeError):
            # On ne casse pas le flux si ça ne parse pas
            return value

        decimals: int = self.config.get("decimals", 2)
        as_string: bool = self.config.get("as_string", True)

        if as_string:
            fmt = "{:." + str(decimals) + "f}"
            return fmt.format(num)
        else:
            # on renvoie un float arrondi
            return round(num, decimals)

    # ----------- Méthode principale ------------

    def run(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        inputs : dictionnaire de données
        Retourne un nouveau dict avec formats normalisés.
        """
        if not isinstance(inputs, dict):
            raise ValueError("FormatterAgent attend un dictionnaire en entrée.")

        result: Dict[str, Any] = {}

        for key, value in inputs.items():
            # 1) nettoyer la chaîne / gérer vide -> None
            cleaned = self._strip_and_empty_to_none(value)

            # 2) formater date si nécessaire
            cleaned = self._format_date(cleaned, key)

            # 3) formater nombre si nécessaire
            cleaned = self._format_number(cleaned, key)

            result[key] = cleaned

        return result
