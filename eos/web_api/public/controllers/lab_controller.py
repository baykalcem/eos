from litestar import Controller, put, get, Response
from litestar.datastructures import State
from litestar.exceptions import HTTPException
from litestar.status_codes import HTTP_200_OK

from eos.web_api.common.entities import LabLoadedStatusesResponse, LabTypes
from eos.web_api.public.exception_handling import handle_exceptions


class LabController(Controller):
    path = "/labs"

    @get("/devices")
    @handle_exceptions("Failed to get lab devices")
    async def get_lab_devices(self, lab_types: list[str] | None, task_type: str | None, state: State) -> Response:
        orchestrator_client = state.orchestrator_client

        params = {}
        if lab_types:
            params["lab_types"] = ",".join(lab_types)
        if task_type:
            params["task_type"] = task_type

        async with orchestrator_client.get("/api/labs/devices", params=params) as response:
            if response.status == HTTP_200_OK:
                lab_devices = await response.json()
                return Response(content=lab_devices, status_code=HTTP_200_OK)

            raise HTTPException(status_code=response.status, detail="Error fetching lab devices")

    @put("/update_loaded")
    @handle_exceptions("Failed to update loaded labs")
    async def update_loaded_labs(self, data: LabTypes, state: State) -> Response:
        orchestrator_client = state.orchestrator_client

        if isinstance(data.lab_types, str):
            if data.lab_types in ["", "[]"]:
                data.lab_types = []
            else:
                data.lab_types = [data.lab_types]

        async with orchestrator_client.put("/api/labs/update_loaded", json={"lab_types": data.lab_types}) as response:
            if response.status == HTTP_200_OK:
                return Response(content={"message": "Labs updated successfully"}, status_code=HTTP_200_OK)

            raise HTTPException(status_code=response.status, detail="Error updating loaded labs")

    @put("/reload")
    @handle_exceptions("Failed to reload labs")
    async def reload_labs(self, data: LabTypes, state: State) -> Response:
        orchestrator_client = state.orchestrator_client

        if isinstance(data.lab_types, str):
            if data.lab_types in ["", "[]"]:
                data.lab_types = []
            else:
                data.lab_types = [data.lab_types]

        async with orchestrator_client.put("/api/labs/reload", json={"lab_types": data.lab_types}) as response:
            if response.status == HTTP_200_OK:
                return Response(content={"message": "Labs reloaded successfully"}, status_code=HTTP_200_OK)

            raise HTTPException(status_code=response.status, detail="Error reloading labs")

    @get("/loaded_statuses")
    @handle_exceptions("Failed to get lab loaded statuses")
    async def get_lab_loaded_statuses(self, state: State) -> LabLoadedStatusesResponse:
        orchestrator_client = state.orchestrator_client
        async with orchestrator_client.get("/api/labs/loaded_statuses") as response:
            if response.status == HTTP_200_OK:
                return LabLoadedStatusesResponse(**await response.json())

            raise HTTPException(status_code=response.status, detail="Error fetching lab loaded statuses")
