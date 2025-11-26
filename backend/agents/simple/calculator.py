from backend.agents.base import AgentBase
from backend.agents.registry import register_agent

@register_agent("calculator")
class CalcAgent(AgentBase):
    def validate(self):
        try:
            expr = self.config.get("expression", "")
            # Simple validation to allow only numbers and operators
            if not all(c in "0123456789+-*/(). " for c in expr):
                raise ValueError("Invalid characters in expression.")
            return True, ""
        except Exception as e:
            return False, str(e)

    def execute(self):
        expr = self.config.get("expression", "")
        try:
            result = eval(expr)
            return {"result": result}
        except Exception as e:
            return {"error": str(e)}