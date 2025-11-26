from agents.base import AgentBase
from agents.registry import register_agent
import requests
from urllib.parse import urlparse


@register_agent("api_caller")
class APICaller(AgentBase):
    """Agent that performs HTTP requests.

    Config keys (all optional except `url` which is required):
    - url: str
    - method: str (default: 'GET')
    - headers: dict
    - data: Any (will be sent as JSON payload)
    - timeout: float (seconds, default: 60)
    """

    def validate(self):
        if not isinstance(self.config, dict):
            raise ValueError("Config must be a dictionary")

        url = self.config.get("url")
        if not url or not isinstance(url, str):
            raise ValueError("URL is required and must be a non-empty string in config")

        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise ValueError("URL must be a valid HTTP/HTTPS URL")

        method = self.config.get("method")
        if method is not None and (not isinstance(method, str) or not method.strip()):
            raise ValueError("If provided, 'method' must be a non-empty string")

    def run(self, inputs):
        inputs = inputs or {}

        # Preference: inputs override config
        def _get(key, default):
            return inputs.get(key, self.config.get(key, default))

        url = _get("url")
        if not url:
            raise ValueError("URL is required for API call")

        method = (_get("method", "GET") or "GET").upper()
        headers = _get("headers", {}) or {}
        data = _get("data", None)
        timeout = _get("timeout", 60)

        try:
            response = requests.request(method, url, headers=headers, json=data, timeout=timeout)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"API request failed: {e}") from e

        try:
            return response.json()
        except ValueError:
            return response.text