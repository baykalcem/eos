import asyncio
from unittest.mock import patch

from eos.experiments.entities.experiment import ExperimentStatus, ExperimentDefinition
from eos.experiments.exceptions import EosExperimentExecutionError
from eos.experiments.experiment_executor import ExperimentExecutor
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
class TestExperimentExecutor:
    @pytest.fixture
    def experiment_executor(
        self,
        experiment_manager,
        task_manager,
        task_executor,
        cp_sat_scheduler,
        experiment_graph,
        db_interface,
    ):
        return ExperimentExecutor(
            experiment_definition=ExperimentDefinition(
                id=EXPERIMENT_ID, type=EXPERIMENT_TYPE, owner="test", dynamic_parameters=DYNAMIC_PARAMETERS
            ),
            experiment_graph=experiment_graph,
            experiment_manager=experiment_manager,
            task_manager=task_manager,
            task_executor=task_executor,
            scheduler=cp_sat_scheduler,
            db_interface=db_interface,
        )

    @pytest.mark.asyncio
    async def test_start_experiment(self, db, experiment_executor, experiment_manager):
        await experiment_executor.start_experiment(db)

        experiment = await experiment_manager.get_experiment(db, EXPERIMENT_ID)
        assert experiment is not None
        assert experiment.id == EXPERIMENT_ID
        assert experiment.status == ExperimentStatus.RUNNING

    @pytest.mark.asyncio
    async def test_task_output_registration(
        self, experiment_executor, resource_allocation_manager, task_executor, task_manager, db_interface
    ):
        async with db_interface.get_async_session() as db:
            await experiment_executor.start_experiment(db)

        experiment_completed = False
        while not experiment_completed:
            async with db_interface.get_async_session() as db:
                experiment_completed = await experiment_executor.progress_experiment(db)
            await task_executor.process_tasks()
            await asyncio.sleep(0.1)

        async with db_interface.get_async_session() as db:
            task = await task_manager.get_task(db, EXPERIMENT_ID, "mixing")
        assert task.output_parameters["mixing_time"] == DYNAMIC_PARAMETERS["mixing"]["time"]

    @pytest.mark.asyncio
    async def test_resolve_input_parameter_references_and_dynamic_parameters(
        self, experiment_executor, resource_allocation_manager, task_executor, task_manager, db_interface
    ):
        async with db_interface.get_async_session() as db:
            await experiment_executor.start_experiment(db)

        experiment_completed = False
        while not experiment_completed:
            async with db_interface.get_async_session() as db:
                experiment_completed = await experiment_executor.progress_experiment(db)
            await task_executor.process_tasks()
            await asyncio.sleep(0.1)

        async with db_interface.get_async_session() as db:
            mixing_task = await task_manager.get_task(db, EXPERIMENT_ID, "mixing")
            evaporation_task = await task_manager.get_task(db, EXPERIMENT_ID, "evaporation")

        # Check the dynamic parameter for input mixing time
        assert mixing_task.input_parameters["time"] == DYNAMIC_PARAMETERS["mixing"]["time"]

        # Check that the output parameter mixing time was assigned to the input parameter evaporation time
        assert evaporation_task.input_parameters["evaporation_time"] == mixing_task.output_parameters["mixing_time"]

    @pytest.mark.parametrize(
        "experiment_status",
        [
            ExperimentStatus.COMPLETED,
            ExperimentStatus.SUSPENDED,
            ExperimentStatus.CANCELLED,
            ExperimentStatus.FAILED,
            ExperimentStatus.RUNNING,
        ],
    )
    @pytest.mark.asyncio
    async def test_handle_existing_experiment(self, db, experiment_executor, experiment_manager, experiment_status):
        await experiment_manager.create_experiment(db, experiment_executor._experiment_definition)
        await experiment_manager._set_experiment_status(db, EXPERIMENT_ID, experiment_status)

        experiment_executor._experiment_definition.resume = False
        with patch.object(experiment_executor, "_resume_experiment") as mock_resume:
            if experiment_status in [
                ExperimentStatus.COMPLETED,
                ExperimentStatus.SUSPENDED,
                ExperimentStatus.CANCELLED,
                ExperimentStatus.FAILED,
            ]:
                with pytest.raises(EosExperimentExecutionError) as exc_info:
                    experiment = await experiment_manager.get_experiment(db, EXPERIMENT_ID)
                    await experiment_executor._handle_existing_experiment(db, experiment)
                assert experiment_status.name.lower() in str(exc_info.value)
                mock_resume.assert_not_called()
            else:
                experiment = await experiment_manager.get_experiment(db, EXPERIMENT_ID)
                await experiment_executor._handle_existing_experiment(db, experiment)
                mock_resume.assert_not_called()

        experiment_executor._experiment_definition.resume = True
        with patch.object(experiment_executor, "_resume_experiment") as mock_resume:
            experiment = await experiment_manager.get_experiment(db, EXPERIMENT_ID)
            await experiment_executor._handle_existing_experiment(db, experiment)
            mock_resume.assert_called_once()

        assert experiment_executor._experiment_status == experiment_status
