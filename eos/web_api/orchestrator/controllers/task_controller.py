from litestar import Controller, Response, get
from litestar.handlers import post
from litestar.status_codes import HTTP_200_OK, HTTP_201_CREATED
from omegaconf import OmegaConf

from eos.orchestration.orchestrator import Orchestrator
from eos.web_api.common.entities import SubmitTaskRequest, TaskTypesResponse
from eos.web_api.public.exception_handling import handle_exceptions


class TaskController(Controller):
    path = "/tasks"

    @get("/{experiment_id:str}/{task_id:str}")
    @handle_exceptions("Failed to get task")
    async def get_task(self, experiment_id: str, task_id: str, orchestrator: Orchestrator) -> Response:
        task = await orchestrator.get_task(experiment_id, task_id)
        return Response(content=task.model_dump_json(), status_code=HTTP_200_OK)

    @post("/submit")
    @handle_exceptions("Failed to submit task")
    async def submit_task(self, data: SubmitTaskRequest, orchestrator: Orchestrator) -> Response:
        await orchestrator.submit_task(
            data.task_config, data.resource_allocation_priority, data.resource_allocation_timeout
        )
        return Response(content=None, status_code=HTTP_201_CREATED)

    @post("/{task_id:str}/cancel")
    @handle_exceptions("Failed to cancel task")
    async def cancel_task(self, task_id: str, orchestrator: Orchestrator) -> Response:
        await orchestrator.cancel_task(task_id)
        return Response(content=None, status_code=HTTP_200_OK)

    @get("/types")
    @handle_exceptions("Failed to get task types")
    async def get_task_types(self, orchestrator: Orchestrator) -> TaskTypesResponse:
        task_types = await orchestrator.get_task_types()
        return TaskTypesResponse(task_types=task_types)

    @get("/{task_type:str}/spec")
    @handle_exceptions("Failed to get task spec")
    async def get_task_spec(self, task_type: str, orchestrator: Orchestrator) -> Response:
        task_spec = await orchestrator.get_task_spec(task_type)
        task_spec = OmegaConf.to_object(task_spec)
        return Response(content=task_spec, status_code=HTTP_200_OK)
