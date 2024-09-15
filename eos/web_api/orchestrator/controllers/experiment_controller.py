from litestar import Controller, get, put, Response
from litestar.handlers import post
from litestar.status_codes import HTTP_200_OK, HTTP_201_CREATED, HTTP_404_NOT_FOUND

from eos.orchestration.orchestrator import Orchestrator
from eos.web_api.common.entities import (
    SubmitExperimentRequest,
    ExperimentTypesResponse,
    ExperimentLoadedStatusesResponse,
    ExperimentTypes,
)
from eos.web_api.public.exception_handling import handle_exceptions


class ExperimentController(Controller):
    path = "/experiments"

    @get("/{experiment_id:str}")
    async def get_experiment(self, experiment_id: str, orchestrator: Orchestrator) -> Response:
        experiment = await orchestrator.get_experiment(experiment_id)

        if experiment is None:
            return Response(content={"error": "Experiment not found"}, status_code=HTTP_404_NOT_FOUND)

        return Response(content=experiment.model_dump_json(), status_code=HTTP_200_OK)

    @post("/submit")
    @handle_exceptions("Failed to submit experiment")
    async def submit_experiment(self, data: SubmitExperimentRequest, orchestrator: Orchestrator) -> Response:
        await orchestrator.submit_experiment(
            data.experiment_id,
            data.experiment_type,
            data.experiment_execution_parameters,
            data.dynamic_parameters,
            data.metadata,
        )
        return Response(content=None, status_code=HTTP_201_CREATED)

    @post("/{experiment_id:str}/cancel")
    @handle_exceptions("Failed to cancel experiment")
    async def cancel_experiment(self, experiment_id: str, orchestrator: Orchestrator) -> Response:
        await orchestrator.cancel_experiment(experiment_id)
        return Response(content=None, status_code=HTTP_200_OK)

    @put("/update_loaded")
    @handle_exceptions("Failed to update loaded experiments")
    async def update_loaded_experiments(self, data: ExperimentTypes, orchestrator: Orchestrator) -> Response:
        await orchestrator.update_loaded_experiments(set(data.experiment_types))
        return Response(content=None, status_code=HTTP_200_OK)

    @put("/reload")
    @handle_exceptions("Failed to reload experiments")
    async def reload_experiments(self, data: ExperimentTypes, orchestrator: Orchestrator) -> Response:
        await orchestrator.reload_experiments(set(data.experiment_types))
        return Response(content=None, status_code=HTTP_200_OK)

    @get("/types")
    @handle_exceptions("Failed to get experiment types")
    async def get_experiment_types(self, orchestrator: Orchestrator) -> ExperimentTypesResponse:
        experiment_types = await orchestrator.get_experiment_types()
        return ExperimentTypesResponse(experiment_types=experiment_types)

    @get("/loaded_statuses")
    @handle_exceptions("Failed to get experiment loaded statuses")
    async def get_experiment_loaded_statuses(self, orchestrator: Orchestrator) -> ExperimentLoadedStatusesResponse:
        experiment_loaded_statuses = await orchestrator.get_experiment_loaded_statuses()
        return ExperimentLoadedStatusesResponse(experiment_loaded_statuses=experiment_loaded_statuses)

    @get("/{experiment_type:str}/dynamic_params_template")
    @handle_exceptions("Failed to get dynamic parameters template")
    async def get_experiment_dynamic_params_template(
        self, experiment_type: str, orchestrator: Orchestrator
    ) -> Response:
        dynamic_params_template = await orchestrator.get_experiment_dynamic_params_template(experiment_type)
        return Response(content=dynamic_params_template, status_code=HTTP_200_OK)
