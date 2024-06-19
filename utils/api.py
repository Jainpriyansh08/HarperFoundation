from dataclasses import dataclass


@dataclass
class Api:
    description: str
    path_params: dict | None = None
    query_params: dict | None = None
    body: dict | None = None
    method: str | None = None
    headers: dict | None = None
    success_response: dict | None = None
    path: str | None = None
    group: str | None = None
    version: str | None = None