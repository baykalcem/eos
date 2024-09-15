from collections.abc import AsyncIterable

from litestar import Controller, get
from litestar.datastructures import State
from litestar.exceptions import HTTPException
from litestar.response import Stream
from litestar.status_codes import HTTP_200_OK

from eos.web_api.public.exception_handling import handle_exceptions


class FileController(Controller):
    path = "/files"

    @get("/download/{experiment_id:str}/{task_id:str}/{file_name:str}")
    @handle_exceptions("Failed to download file")
    async def download_task_output_file(self, experiment_id: str, task_id: str, file_name: str, state: State) -> Stream:
        orchestrator_client = state.orchestrator_client

        async def file_stream() -> AsyncIterable:
            async with orchestrator_client.get(
                f"/api/files/download/{experiment_id}/{task_id}/{file_name}", chunked=True
            ) as response:
                if response.status == HTTP_200_OK:
                    async for chunk in response.content.iter_any():
                        yield chunk
                else:
                    raise HTTPException(status_code=response.status, detail="Error downloading file")

        return Stream(file_stream(), headers={"Content-Disposition": f"attachment; filename={file_name}"})

    @get("/download/{experiment_id:str}/{task_id:str}")
    @handle_exceptions("Failed to download zipped task output files")
    async def download_task_output_files_zipped(self, experiment_id: str, task_id: str, state: State) -> Stream:
        orchestrator_client = state.orchestrator_client

        async def zip_stream() -> AsyncIterable:
            async with orchestrator_client.get(
                f"/api/files/download/{experiment_id}/{task_id}", chunked=True
            ) as response:
                if response.status == HTTP_200_OK:
                    async for chunk in response.content.iter_any():
                        yield chunk
                else:
                    raise HTTPException(status_code=response.status, detail="Error downloading zipped files")

        return Stream(
            zip_stream(), headers={"Content-Disposition": f"attachment; filename={experiment_id}_{task_id}_output.zip"}
        )
