import asyncio

from eos.experiments.entities.experiment import ExperimentStatus
from eos.tasks.entities.task import TaskStatus
from tests.fixtures import *

LAB_ID = "small_lab"
EXPERIMENT_TYPE = "water_purification"
EXPERIMENT_ID = "water_purification_#1"

DYNAMIC_PARAMETERS = {
    "mixing": {
        "time": 120,
    },
    "evaporation": {
        "evaporation_temperature": 120,
        "evaporation_rotation_speed": 200,
        "evaporation_sparging_flow": 5,
    },
}


@pytest.mark.parametrize(
    "setup_lab_experiment",
    [(LAB_ID, EXPERIMENT_TYPE)],
    indirect=True,
)
@pytest.mark.parametrize(
    "experiment_executor",
    [(EXPERIMENT_ID, EXPERIMENT_TYPE)],
    indirect=True,
)
class TestExperimentExecutor:
    def test_start_experiment(self, experiment_executor, experiment_manager):
        experiment_executor.start_experiment(DYNAMIC_PARAMETERS)

        experiment = experiment_manager.get_experiment(EXPERIMENT_ID)
        assert experiment is not None
        assert experiment.id == EXPERIMENT_ID
        assert experiment.status == ExperimentStatus.RUNNING

    @pytest.mark.asyncio
    async def test_progress_experiment(self, experiment_executor, experiment_manager, task_manager):
        experiment_executor.start_experiment(DYNAMIC_PARAMETERS)

        experiment_completed = await experiment_executor.progress_experiment()
        assert not experiment_completed
        await experiment_executor._task_output_futures["mixing"]

        experiment_completed = await experiment_executor.progress_experiment()
        assert not experiment_completed
        task = task_manager.get_task(EXPERIMENT_ID, "mixing")
        assert task is not None
        assert task.status == TaskStatus.COMPLETED
        await experiment_executor._task_output_futures["evaporation"]

        experiment_completed = await experiment_executor.progress_experiment()
        task = task_manager.get_task(EXPERIMENT_ID, "evaporation")
        assert task.status == TaskStatus.COMPLETED
        assert not experiment_completed

        # Final progress
        experiment_completed = await experiment_executor.progress_experiment()
        assert experiment_completed
        experiment = experiment_manager.get_experiment(EXPERIMENT_ID)
        assert experiment.status == ExperimentStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_task_output_registration(self, experiment_executor, task_manager):
        experiment_executor.start_experiment(DYNAMIC_PARAMETERS)

        experiment_completed = False
        while not experiment_completed:
            experiment_completed = await experiment_executor.progress_experiment()
            await asyncio.sleep(0.1)

        mixing_output = task_manager.get_task_output(EXPERIMENT_ID, "mixing")
        assert mixing_output is not None
        assert mixing_output.parameters["mixing_time"] == DYNAMIC_PARAMETERS["mixing"]["time"]

    @pytest.mark.asyncio
    async def test_resolve_input_parameter_references_and_dynamic_parameters(
        self, experiment_executor, task_manager
    ):
        experiment_executor.start_experiment(DYNAMIC_PARAMETERS)

        experiment_completed = False
        while not experiment_completed:
            experiment_completed = await experiment_executor.progress_experiment()
            await asyncio.sleep(0.1)

        mixing_task = task_manager.get_task(EXPERIMENT_ID, "mixing")
        mixing_result = task_manager.get_task_output(EXPERIMENT_ID, "mixing")

        evaporation_task = task_manager.get_task(EXPERIMENT_ID, "evaporation")
        # Check the dynamic parameter for input mixing time
        assert mixing_task.input.parameters["time"] == DYNAMIC_PARAMETERS["mixing"]["time"]

        # Check that the output parameter mixing time was assigned to the input parameter evaporation time
        assert (
            evaporation_task.input.parameters["evaporation_time"]
            == mixing_result.parameters["mixing_time"]
        )
