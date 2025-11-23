class AgentBase:
    def __init__(self, config: dict):
        self.config = config

    def validate(self):
        raise NotImplementedError

    def run(self, inputs):
        raise NotImplementedError
