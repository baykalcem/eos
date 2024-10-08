from typing import Any

import ray
from ray.actor import ActorHandle


class RayActorWrapper:
    """Wrapper for Ray actors to allow for easy synchronous calls to actor methods."""

    def __init__(self, actor: ActorHandle):
        self._actor = actor

    def __getattr__(self, name: str) -> Any:
        if not name.startswith("__"):
            async_func = getattr(self._actor, name)

            def wrapper(*args, **kwargs) -> Any:
                return ray.get(async_func.remote(*args, **kwargs))

            return wrapper

        return super().__getattr__(name)

    @property
    def actor(self) -> ActorHandle:
        return self._actor
