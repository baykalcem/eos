from dataclasses import dataclass


@dataclass
class ServiceCredentials:
    host: str
    port: int
    username: str
    password: str
