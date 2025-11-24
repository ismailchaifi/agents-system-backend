import json
from backend.agents.base import AgentBase
from backend.agents.registry import register_agent

@register_agent("data_parser")
class DataParserAgent(AgentBase):

    def validate(self):
        if "extract_fields" not in self.config:
            raise ValueError("extract_fields est obligatoire")
        if not isinstance(self.config["extract_fields"], list):
            raise ValueError("extract_fields doit être une liste")

    def run(self, inputs):
        if isinstance(inputs, str):
            try:
                inputs = json.loads(inputs)
            except:
                raise ValueError("Invalid JSON input")

        data = {}

        for field in self.config["extract_fields"]:
            if field in inputs:
                data[field] = inputs[field]

        if self.config.get("remove_nulls", True):
            data = {k: v for k, v in data.items() if v is not None}

        return data
