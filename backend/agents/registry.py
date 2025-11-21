AGENT_REGISTRY = {}

def register_agent(name, agent_class):
    AGENT_REGISTRY[name] = agent_class

def get_agent(name):
    return AGENT_REGISTRY.get(name)
