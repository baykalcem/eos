import asyncio

from eos.configuration.entities.task import TaskConfig, TaskDeviceConfig
from eos.resource_allocation.entities.resource_request import (
    ResourceAllocationRequest,
    ResourceType,
)
from eos.tasks.entities.task_execution_parameters import TaskExecutionParameters
from eos.tasks.exceptions import EosTaskResourceAllocationError
from tests.fixtures import *


@pytest.mark.parametrize(
    "setup_lab_experiment",
    [("small_lab", "water_purification")],
    indirect=True,
)
class TestTaskExecutor:
    @pytest.mark.asyncio
    async def test_request_task_execution(
        self,
        task_executor,
        experiment_manager,
        experiment_graph,
    ):
        experiment_manager.create_experiment("water_purification", "water_purification")

        task_config = experiment_graph.get_task_config("mixing")
        task_config.parameters["time"] = 5

        task_parameters = TaskExecutionParameters(
            experiment_id="water_purification",
            devices=[TaskDeviceConfig(lab_id="small_lab", id="magnetic_mixer")],
            task_config=task_config,
        )
        task_output_parameters, _, _ = await task_executor.request_task_execution(task_parameters)
        assert task_output_parameters["mixing_time"] == 5

        task_parameters.task_config.id = "mixing2"
        task_output_parameters, _, _ = await task_executor.request_task_execution(task_parameters)
        assert task_output_parameters["mixing_time"] == 5

        task_parameters.task_config.id = "mixing3"
        task_output_parameters, _, _ = await task_executor.request_task_execution(task_parameters)
        assert task_output_parameters["mixing_time"] == 5

    @pytest.mark.asyncio
    async def test_request_task_execution_resource_request_timeout(
        self,
        task_executor,
        experiment_manager,
        experiment_graph,
        resource_allocation_manager,
    ):
        request = ResourceAllocationRequest(
            requester="tester",
        )
        request.add_resource("magnetic_mixer", "small_lab", ResourceType.DEVICE)
        active_request = resource_allocation_manager.request_resources(request, lambda requests: None)
        resource_allocation_manager.process_active_requests()

        experiment_manager.create_experiment("water_purification", "water_purification")

        task_config = experiment_graph.get_task_config("mixing")
        task_config.parameters["time"] = 5
        task_parameters = TaskExecutionParameters(
            experiment_id="water_purification",
            devices=[TaskDeviceConfig(lab_id="small_lab", id="magnetic_mixer")],
            task_config=task_config,
            resource_allocation_timeout=1,
        )
        with pytest.raises(EosTaskResourceAllocationError):
            await task_executor.request_task_execution(task_parameters)

        resource_allocation_manager.release_resources(active_request)

    @pytest.mark.asyncio
    async def test_request_task_cancellation(self, task_executor, experiment_manager):
        experiment_manager.create_experiment("water_purification", "water_purification")

        sleep_config = TaskConfig(
            id="sleep_task",
            type="Sleep",
            devices=[TaskDeviceConfig(lab_id="small_lab", id="general_computer")],
            parameters={"sleep_time": 2},
        )
        task_parameters = TaskExecutionParameters(
            experiment_id="water_purification",
            task_config=sleep_config,
        )

        tasks = set()

        task = asyncio.create_task(task_executor.request_task_execution(task_parameters))
        tasks.add(task)
        await asyncio.sleep(1)

        await task_executor.request_task_cancellation(task_parameters.experiment_id, task_parameters.task_config.id)

        assert True
