from tests.fixtures import *


@pytest.fixture()
def experiment_graph(configuration_manager, basic_scheduler):
    experiment = configuration_manager.experiments["abstract_experiment"]
    return ExperimentGraph(experiment)


@pytest.mark.parametrize("setup_lab_experiment", [("abstract_lab", "abstract_experiment")], indirect=True)
class TestBasicScheduler:
    def test_register_experiment(self, basic_scheduler, experiment_graph, configuration_manager):
        print(configuration_manager.device_specs)
        basic_scheduler.register_experiment("experiment_1", "abstract_experiment", experiment_graph)
        assert basic_scheduler._registered_experiments["experiment_1"] == (
            "abstract_experiment",
            experiment_graph,
        )

    def test_unregister_experiment(self, basic_scheduler, experiment_graph):
        basic_scheduler.register_experiment("experiment_1", "abstract_experiment", experiment_graph)
        basic_scheduler.unregister_experiment("experiment_1")
        assert "experiment_1" not in basic_scheduler._registered_experiments

    @pytest.mark.asyncio
    async def test_correct_schedule(self, basic_scheduler, experiment_graph, experiment_manager, task_manager):
        def complete_task(task_id, task_type):
            task_manager.create_task("experiment_1", task_id, task_type, [])
            task_manager.start_task("experiment_1", task_id)
            task_manager.complete_task("experiment_1", task_id)

        def get_task_if_exists(tasks, task_id):
            return next((task for task in tasks if task.id == task_id), None)

        def assert_task(task, task_id, device_lab_id, device_id):
            assert task.id == task_id
            assert task.devices[0].lab_id == device_lab_id
            assert task.devices[0].id == device_id

        def process_and_assert(tasks, expected_tasks):
            assert len(tasks) == len(expected_tasks)
            for task_id, device_lab_id, device_id in expected_tasks:
                task = get_task_if_exists(tasks, task_id)
                assert_task(task, task_id, device_lab_id, device_id)
                complete_task(task_id, "Noop")

        experiment_manager.create_experiment("experiment_1", "abstract_experiment")
        experiment_manager.start_experiment("experiment_1")
        basic_scheduler.register_experiment("experiment_1", "abstract_experiment", experiment_graph)

        tasks = await basic_scheduler.request_tasks("experiment_1")
        process_and_assert(tasks, [("A", "abstract_lab", "D2")])

        tasks = await basic_scheduler.request_tasks("experiment_1")
        process_and_assert(tasks, [("B", "abstract_lab", "D1"), ("C", "abstract_lab", "D3")])

        tasks = await basic_scheduler.request_tasks("experiment_1")
        process_and_assert(
            tasks,
            [("D", "abstract_lab", "D1"), ("E", "abstract_lab", "D3"), ("F", "abstract_lab", "D2")],
        )

        tasks = await basic_scheduler.request_tasks("experiment_1")
        process_and_assert(tasks, [("G", "abstract_lab", "D5")])

        tasks = await basic_scheduler.request_tasks("experiment_1")
        process_and_assert(tasks, [("H", "abstract_lab", "D6")])

        assert basic_scheduler.is_experiment_completed("experiment_1")

        tasks = await basic_scheduler.request_tasks("experiment_1")
        assert len(tasks) == 0
        experiment_manager.complete_experiment("experiment_1")
