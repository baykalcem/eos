from litestar import Controller, put, get, Response
from litestar.status_codes import HTTP_200_OK
from omegaconf import OmegaConf

from eos.orchestration.orchestrator import Orchestrator
from eos.web_api.common.entities import LabLoadedStatusesResponse, LabTypes
from eos.web_api.public.exception_handling import handle_exceptions


class LabController(Controller):
    path = "/labs"

    @get("/devices")
    @handle_exceptions("Failed to get lab devices")
    async def get_lab_devices(
        self, lab_types: list[str] | None, task_type: str | None, orchestrator: Orchestrator
    ) -> Response:
        lab_devices = await orchestrator.get_lab_devices(lab_types, task_type)

        # Convert LabDeviceConfig objects to plain dictionaries
        dict_lab_devices = {}
        for lab_type, devices in lab_devices.items():
            dict_lab_devices[lab_type] = {name: OmegaConf.to_object(device) for name, device in devices.items()}

        return Response(content=dict_lab_devices, status_code=HTTP_200_OK)

    @put("/update_loaded")
    @handle_exceptions("Failed to update loaded labs")
    async def update_loaded_labs(self, data: LabTypes, orchestrator: Orchestrator) -> Response:
        await orchestrator.update_loaded_labs(set(data.lab_types))
        return Response(content=None, status_code=HTTP_200_OK)

    @put("/reload")
    @handle_exceptions("Failed to reload labs")
    async def reload_labs(self, data: LabTypes, orchestrator: Orchestrator) -> Response:
        await orchestrator.reload_labs(set(data.lab_types))
        return Response(content=None, status_code=HTTP_200_OK)

    @get("/loaded_statuses")
    @handle_exceptions("Failed to get lab loaded statuses")
    async def get_lab_loaded_statuses(self, orchestrator: Orchestrator) -> LabLoadedStatusesResponse:
        lab_loaded_statuses = await orchestrator.get_lab_loaded_statuses()
        return LabLoadedStatusesResponse(lab_loaded_statuses=lab_loaded_statuses)
