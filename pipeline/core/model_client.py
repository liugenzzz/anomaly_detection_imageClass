from __future__ import annotations

import json
import ssl
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class ProviderError(RuntimeError):
    pass


def call_chat_completion(provider: dict, messages: list[dict], retries: int, retry_sleep: float) -> dict:
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            return _call_once(provider, messages)
        # urllib normally wraps transport failures in URLError, but TLS
        # decoding/handshake failures can escape as a bare SSLError.  Treat
        # those the same as other transient network errors so that one bad
        # connection does not immediately count as a provider failure.
        except (HTTPError, URLError, TimeoutError, ssl.SSLError, ProviderError) as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(retry_sleep * (attempt + 1))
    raise ProviderError(f"{provider['name']} failed after {retries + 1} attempts: {last_error}")


def _call_once(provider: dict, messages: list[dict]) -> dict:
    body = {
        "model": provider["model"],
        "messages": messages,
        "stream": provider.get("stream", False),
        "temperature": provider.get("temperature", 0.4),
        "max_tokens": provider.get("max_tokens", 8192),
    }
    if provider.get("chat_template_kwargs"):
        body["chat_template_kwargs"] = provider["chat_template_kwargs"]

    headers = {"Content-Type": "application/json"}
    if provider.get("api_key"):
        headers["Authorization"] = f"Bearer {provider['api_key']}"

    if provider.get("transport", "urllib") == "requests":
        payload = _post_with_requests(provider, body, headers)
    else:
        payload = _post_with_urllib(provider, body, headers)

    return _parse_response(provider, payload)


def _post_with_urllib(provider: dict, body: dict, headers: dict) -> str:
    request = Request(
        provider["url"],
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    with urlopen(request, timeout=provider.get("timeout", 2400)) as response:
        return response.read().decode("utf-8")


def _post_with_requests(provider: dict, body: dict, headers: dict) -> str:
    """POST through requests for gateways incompatible with urllib TLS I/O."""
    try:
        import requests
    except ImportError as exc:
        raise ProviderError(
            f"{provider['name']} requires the 'requests' package"
        ) from exc

    try:
        response = requests.post(
            provider["url"],
            headers=headers,
            json=body,
            timeout=provider.get("timeout", 2400),
        )
        response.raise_for_status()
        return response.text
    except requests.RequestException as exc:
        raise ProviderError(
            f"{provider['name']} requests transport failed: {exc}"
        ) from exc


def _parse_response(provider: dict, payload: str) -> dict:

    try:
        response_json = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ProviderError(f"{provider['name']} returned non-JSON response") from exc

    content = (
        response_json.get("choices", [{}])[0]
        .get("message", {})
        .get("content", "")
    )
    if not content:
        raise ProviderError(f"{provider['name']} returned empty content")

    return {
        "provider": provider["name"],
        "model": provider["model"],
        "weight": provider.get("weight", 1),
        "content": content,
        "raw_response": response_json,
    }
