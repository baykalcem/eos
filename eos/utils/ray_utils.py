from typing import Any

import ray
from ray.actor import ActorHandle


class RayActorWrapper:
    """
    Wrapper for Ray actors to allow for easy synchronous calls to actor methods.
    """

    def __init__(self, actor: ActorHandle):
        self.actor = actor

    def __getattr__(self, name: str) -> Any:
        if not name.startswith("__"):
            async_func = getattr(self.actor, name)

            def wrapper(*args, **kwargs) -> Any:
                return ray.get(async_func.remote(*args, **kwargs))

            return wrapper

        return super().__getattr__(name)


def ray_run(ray_remote_method: callable, *args, **kwargs) -> Any:
    """
    A helper function to simplify calling Ray remote functions.

    Args:
        ray_remote_method: The Ray remote method to be invoked.
        *args: Arguments to be passed to the remote method.
        **kwargs: Keyword arguments to be passed to the remote method.

    Returns:
        The result of the Ray remote method call.
    """
    # Invoke the remote method and get the result
    return ray.get(ray_remote_method.remote(*args, **kwargs))
