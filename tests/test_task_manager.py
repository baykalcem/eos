from eos.tasks.entities.task import TaskStatus, TaskOutput
from eos.tasks.exceptions import EosTaskStateError, EosTaskExistsError
from tests.fixtures import *

EXPERIMENT_ID = "water_purification"


@pytest.fixture
def experiment_manager(configuration_manager, db_manager):
    experiment_manager = ExperimentManager(configuration_manager, db_manager)
    experiment_manager.create_experiment(EXPERIMENT_ID, "water_purification")
    return experiment_manager


@pytest.mark.parametrize("setup_lab_experiment", [("small_lab", "water_purification")], indirect=True)
class TestTaskManager:
    def test_create_task(self, task_manager, experiment_manager):
        task_manager.create_task(EXPERIMENT_ID, "mixing", "Magnetic Mixing", [])

        task = task_manager.get_task(EXPERIMENT_ID, "mixing")
        assert task.id == "mixing"
        assert task.type == "Magnetic Mixing"

    def test_create_task_nonexistent(self, task_manager, experiment_manager):
        with pytest.raises(EosTaskStateError):
            task_manager.create_task(EXPERIMENT_ID, "nonexistent", "nonexistent", [])

    def test_create_task_nonexistent_task_type(self, task_manager, experiment_manager):
        with pytest.raises(EosTaskStateError):
            task_manager.create_task(EXPERIMENT_ID, "nonexistent_task", "Nonexistent", [])

    def test_create_existing_task(self, task_manager, experiment_manager):
        task_manager.create_task(EXPERIMENT_ID, "mixing", "Magnetic Mixing", [])

        with pytest.raises(EosTaskExistsError):
            task_manager.create_task(EXPERIMENT_ID, "mixing", "Magnetic Mixing", [])

    def test_delete_task(self, task_manager):
        task_manager.create_task(EXPERIMENT_ID, "mixing", "Magnetic Mixing", [])

        task_manager.delete_task(EXPERIMENT_ID, "mixing")

        assert task_manager.get_task(EXPERIMENT_ID, "mixing") is None

    def test_delete_nonexistent_task(self, task_manager, experiment_manager):
        with pytest.raises(EosTaskStateError):
            task_manager.delete_task(EXPERIMENT_ID, "nonexistent_task")

    def test_get_all_tasks_by_status(self, task_manager, experiment_manager):
        task_manager.create_task(EXPERIMENT_ID, "mixing", "Magnetic Mixing", [])
        task_manager.create_task(EXPERIMENT_ID, "purification", "Purification", [])

        task_manager.start_task(EXPERIMENT_ID, "mixing")
        task_manager.complete_task(EXPERIMENT_ID, "purification")

        assert len(task_manager.get_tasks(experiment_id=EXPERIMENT_ID, status=TaskStatus.RUNNING.value)) == 1
        assert len(task_manager.get_tasks(experiment_id=EXPERIMENT_ID, status=TaskStatus.COMPLETED.value)) == 1

    def test_set_task_status(self, task_manager, experiment_manager):
        task_manager.create_task(EXPERIMENT_ID, "mixing", "Magnetic Mixing", [])

        assert task_manager.get_task(EXPERIMENT_ID, "mixing").status == TaskStatus.CREATED

        task_manager.start_task(EXPERIMENT_ID, "mixing")
        assert task_manager.get_task(EXPERIMENT_ID, "mixing").status == TaskStatus.RUNNING

        task_manager.complete_task(EXPERIMENT_ID, "mixing")
        assert task_manager.get_task(EXPERIMENT_ID, "mixing").status == TaskStatus.COMPLETED

    def test_set_task_status_nonexistent_task(self, task_manager, experiment_manager):
        with pytest.raises(EosTaskStateError):
            task_manager.start_task(EXPERIMENT_ID, "nonexistent_task")

    def test_start_task(self, task_manager, experiment_manager):
        task_manager.create_task(EXPERIMENT_ID, "mixing", "Magnetic Mixing", [])

        task_manager.start_task(EXPERIMENT_ID, "mixing")
        assert "mixing" in experiment_manager.get_running_tasks(EXPERIMENT_ID)

    def test_start_task_nonexistent_experiment(self, task_manager, experiment_manager):
        with pytest.raises(EosTaskStateError):
            task_manager.start_task(EXPERIMENT_ID, "nonexistent_task")

    def test_complete_task(self, task_manager, experiment_manager):
        task_manager.create_task(EXPERIMENT_ID, "mixing", "Magnetic Mixing", [])
        task_manager.start_task(EXPERIMENT_ID, "mixing")
        task_manager.complete_task(EXPERIMENT_ID, "mixing")
        assert "mixing" not in experiment_manager.get_running_tasks(EXPERIMENT_ID)
        assert "mixing" in experiment_manager.get_completed_tasks(EXPERIMENT_ID)

    def test_complete_task_nonexistent_experiment(self, task_manager, experiment_manager):
        with pytest.raises(EosTaskStateError):
            task_manager.complete_task(EXPERIMENT_ID, "nonexistent_task")

    def test_add_task_output(self, task_manager):
        task_manager.create_task(EXPERIMENT_ID, "mixing", "Magnetic Mixing", [])

        task_output = TaskOutput(
            experiment_id=EXPERIMENT_ID,
            task_id="mixing",
            parameters={"x": 5},
            file_names=["file"],
        )
        task_manager.add_task_output(EXPERIMENT_ID, "mixing", task_output)
        task_manager.add_task_output_file(EXPERIMENT_ID, "mixing", "file", b"file_data")

        output = task_manager.get_task_output(experiment_id=EXPERIMENT_ID, task_id="mixing")
        assert output.parameters == {"x": 5}
        assert output.file_names == ["file"]

        output_file = task_manager.get_task_output_file(experiment_id=EXPERIMENT_ID, task_id="mixing", file_name="file")
        assert output_file == b"file_data"
