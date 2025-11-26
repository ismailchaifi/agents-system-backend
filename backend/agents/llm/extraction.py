import json
from typing import Any, Dict, List

from openai import OpenAI

from backend.agents.base import AgentBase
from backend.agents.registry import register_agent

# Client OpenAI — nécessite OPENAI_API_KEY dans l'environnement
client = OpenAI()


@register_agent("extraction")
class ExtractionAgent(AgentBase):
    """
    Agent LLM pour extraire des informations structurées à partir d'un texte.
    """

    def validate(self):
        fields = self.config.get("fields")
        if not fields or not isinstance(fields, list):
            raise ValueError("'fields' est obligatoire et doit être une liste.")

        if not isinstance(self.config.get("model", "gpt-4o-mini"), str):
            raise ValueError("'model' doit être une chaîne de caractères.")

    # -----------------------
    # PROMPTS
    # -----------------------

    def _build_system_prompt(self) -> str:
        fields: List[str] = self.config["fields"]
        extra_instructions: str = self.config.get("instructions", "")

        fields_str = ", ".join(f'"{f}"' for f in fields)

        return f"""
Tu es un extracteur d'information STRICTEMENT structuré.

Tu reçois un texte brut (email, OCR de facture, message, etc.).
Tu dois renvoyer un JSON avec uniquement les champs suivants :

[{fields_str}]

Règles :
- Si une information est absente ou introuvable, mets la valeur null.
- Ne fais AUCUNE supposition.
- Ne renvoie qu'un JSON valide, sans texte autour.

{extra_instructions}
""".strip()

    def _build_user_prompt(self, text: str) -> str:
        return f"Voici le texte d'entrée :\n{text}"

    # -----------------------
    # APPEL AU LLM
    # -----------------------

    def _call_llm(self, text: str) -> Dict[str, Any]:
        model = self.config.get("model", "gpt-4o-mini")

        response = client.chat.completions.create(
            model=model,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": self._build_system_prompt()},
                {"role": "user", "content": self._build_user_prompt(text)},
            ],
        )

        content = response.choices[0].message.content

        try:
            return json.loads(content)
        except json.JSONDecodeError:
            raise ValueError(f"Sortie non JSON du LLM : {content}")

    # -----------------------
    # LOGIQUE PRINCIPALE
    # -----------------------

    def run(self, inputs: Any) -> Dict[str, Any]:
        """
        inputs : texte brut (str) ou dict avec clé 'text'
        retourne : dict avec les champs demandés
        """
        if isinstance(inputs, dict):
            text = inputs.get("text", "")
        else:
            text = str(inputs)

        if not text.strip():
            raise ValueError("ExtractionAgent: texte d'entrée vide.")

        extracted = self._call_llm(text)

        # Normaliser : garantir que tous les champs existent
        fields: List[str] = self.config["fields"]
        final: Dict[str, Any] = {}
        for field in fields:
            final[field] = extracted.get(field)

        return final
