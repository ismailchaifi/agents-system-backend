AGENT_REGISTRY = {}


def register_agent(name):
    """Register an agent class under `name`.

    Usage:
    - As a decorator: `@register_agent("my_agent")` above the class definition.
    """
    if name is None:
        raise ValueError("register_agent requires a name when used as a decorator")

    def _decorator(cls):
        AGENT_REGISTRY[name] = cls
        return cls

    return _decorator


def get_agent(name):
    return AGENT_REGISTRY.get(name)
