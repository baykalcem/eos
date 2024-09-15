from eos.experiments.entities.experiment import ExperimentStatus
from eos.experiments.exceptions import EosExperimentStateError
from tests.fixtures import *

EXPERIMENT_ID = "water_purification"


@pytest.mark.parametrize("setup_lab_experiment", [("small_lab", EXPERIMENT_ID)], indirect=True)
class TestExperimentManager:
    def test_create_experiment(self, experiment_manager):
        experiment_manager.create_experiment("test_experiment", EXPERIMENT_ID)
        experiment_manager.create_experiment("test_experiment_2", EXPERIMENT_ID)

        assert experiment_manager.get_experiment("test_experiment").id == "test_experiment"
        assert experiment_manager.get_experiment("test_experiment_2").id == "test_experiment_2"

    def test_create_experiment_nonexistent_type(self, experiment_manager):
        with pytest.raises(EosExperimentStateError):
            experiment_manager.create_experiment("test_experiment", "nonexistent_type")

    def test_create_existing_experiment(self, experiment_manager):
        experiment_manager.create_experiment("test_experiment", EXPERIMENT_ID)

        with pytest.raises(EosExperimentStateError):
            experiment_manager.create_experiment("test_experiment", EXPERIMENT_ID)

    def test_delete_experiment(self, experiment_manager):
        experiment_manager.create_experiment("test_experiment", EXPERIMENT_ID)

        assert experiment_manager.get_experiment("test_experiment").id == "test_experiment"

        experiment_manager.delete_experiment("test_experiment")

        assert experiment_manager.get_experiment("test_experiment") is None

    def test_delete_nonexisting_experiment(self, experiment_manager):
        with pytest.raises(EosExperimentStateError):
            experiment_manager.delete_experiment("non_existing_experiment")

    def test_get_experiments_by_status(self, experiment_manager):
        experiment_manager.create_experiment("test_experiment", EXPERIMENT_ID)
        experiment_manager.create_experiment("test_experiment_2", EXPERIMENT_ID)
        experiment_manager.create_experiment("test_experiment_3", EXPERIMENT_ID)

        experiment_manager.start_experiment("test_experiment")
        experiment_manager.start_experiment("test_experiment_2")
        experiment_manager.complete_experiment("test_experiment_3")

        running_experiments = experiment_manager.get_experiments(
            status=ExperimentStatus.RUNNING.value
        )
        completed_experiments = experiment_manager.get_experiments(
            status=ExperimentStatus.COMPLETED.value
        )

        assert running_experiments == [
            experiment_manager.get_experiment("test_experiment"),
            experiment_manager.get_experiment("test_experiment_2"),
        ]

        assert completed_experiments == [experiment_manager.get_experiment("test_experiment_3")]

    def test_set_experiment_status(self, experiment_manager):
        experiment_manager.create_experiment("test_experiment", EXPERIMENT_ID)
        assert (
            experiment_manager.get_experiment("test_experiment").status == ExperimentStatus.CREATED
        )

        experiment_manager.start_experiment("test_experiment")
        assert (
            experiment_manager.get_experiment("test_experiment").status == ExperimentStatus.RUNNING
        )

        experiment_manager.complete_experiment("test_experiment")
        assert (
            experiment_manager.get_experiment("test_experiment").status
            == ExperimentStatus.COMPLETED
        )

    def test_set_experiment_status_nonexistent_experiment(self, experiment_manager):
        with pytest.raises(EosExperimentStateError):
            experiment_manager.start_experiment("nonexistent_experiment")

    def test_get_all_experiments(self, experiment_manager):
        experiment_manager.create_experiment("test_experiment", EXPERIMENT_ID)
        experiment_manager.create_experiment("test_experiment_2", EXPERIMENT_ID)
        experiment_manager.create_experiment("test_experiment_3", EXPERIMENT_ID)

        assert experiment_manager.get_experiments() == [
            experiment_manager.get_experiment("test_experiment"),
            experiment_manager.get_experiment("test_experiment_2"),
            experiment_manager.get_experiment("test_experiment_3"),
        ]
