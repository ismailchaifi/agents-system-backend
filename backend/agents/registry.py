AGENT_REGISTRY = {}


def register_agent(name):
    """
    Décorateur pour enregistrer une classe d'agent sous un nom donné.

    Exemple d'utilisation :
    
    @register_agent("data_parser")
    class DataParserAgent(AgentBase):
        ...
    """

    def decorator(cls):
        AGENT_REGISTRY[name] = cls
        return cls

    return decorator


def get_agent(name):
    """Récupère une classe d'agent à partir de son nom."""
    return AGENT_REGISTRY.get(name)
