import os
from pathlib import Path

import pytest
import ray
import yaml

from eos.campaigns.campaign_executor import CampaignExecutor
from eos.campaigns.campaign_manager import CampaignManager
from eos.campaigns.campaign_optimizer_manager import CampaignOptimizerManager
from eos.campaigns.entities.campaign import CampaignExecutionParameters
from eos.configuration.configuration_manager import ConfigurationManager
from eos.configuration.experiment_graph.experiment_graph import ExperimentGraph
from eos.containers.container_manager import ContainerManager
from eos.devices.device_manager import DeviceManager
from eos.experiments.entities.experiment import ExperimentExecutionParameters
from eos.experiments.experiment_executor import ExperimentExecutor
from eos.experiments.experiment_executor_factory import ExperimentExecutorFactory
from eos.experiments.experiment_manager import ExperimentManager
from eos.logging.logger import log
from eos.persistence.db_manager import DbManager
from eos.persistence.file_db_manager import FileDbManager
from eos.persistence.service_credentials import ServiceCredentials
from eos.resource_allocation.container_allocation_manager import ContainerAllocationManager
from eos.resource_allocation.device_allocation_manager import DeviceAllocationManager
from eos.resource_allocation.resource_allocation_manager import (
    ResourceAllocationManager,
)
from eos.scheduling.basic_scheduler import BasicScheduler
from eos.tasks.task_executor import TaskExecutor
from eos.tasks.task_manager import TaskManager

log.set_level("INFO")


def load_test_config(config_name):
    config_path = Path(__file__).resolve().parent / "test_config.yaml"

    if not config_path.exists():
        raise FileNotFoundError(f"Test config file not found at {config_path}")

    with Path(config_path).open("r") as file:
        config = yaml.safe_load(file)

    if config_name not in config:
        raise KeyError(f"Config key {config_name} not found in test config file")

    return config.get(config_name)


@pytest.fixture(scope="session")
def configuration_manager():
    config = load_test_config("configuration_manager")
    root_dir = Path(__file__).resolve().parent.parent
    user_dir = root_dir / config["user_dir"]
    os.chdir(root_dir)
    return ConfigurationManager(user_dir=str(user_dir))


@pytest.fixture(scope="session")
def task_specification_registry(configuration_manager):
    return configuration_manager.task_specs


@pytest.fixture
def user_dir():
    config = load_test_config("configuration_manager")
    root_dir = Path(__file__).resolve().parent.parent
    return root_dir / config["user_dir"]


@pytest.fixture(scope="session")
def db_manager():
    config = load_test_config("db_manager")

    db_credentials_config = config["db_credentials"]
    db_credentials = ServiceCredentials(
        host=db_credentials_config["host"],
        port=db_credentials_config["port"],
        username=db_credentials_config["username"],
        password=db_credentials_config["password"],
    )

    return DbManager(db_credentials, "test-eos")


@pytest.fixture(scope="session")
def file_db_manager(db_manager):
    config = load_test_config("file_db_manager")

    file_db_credentials_config = config["file_db_credentials"]
    file_db_credentials = ServiceCredentials(
        host=file_db_credentials_config["host"],
        port=file_db_credentials_config["port"],
        username=file_db_credentials_config["username"],
        password=file_db_credentials_config["password"],
    )

    return FileDbManager(file_db_credentials, bucket_name="test-eos")


@pytest.fixture
def setup_lab_experiment(request, configuration_manager, db_manager):
    lab_name, experiment_name = request.param

    if lab_name not in configuration_manager.labs:
        configuration_manager.load_lab(lab_name)
    lab_config = configuration_manager.labs[lab_name]

    if experiment_name not in configuration_manager.experiments:
        configuration_manager.load_experiment(experiment_name)
    experiment_config = configuration_manager.experiments[experiment_name]

    return lab_config, experiment_config


@pytest.fixture
def experiment_graph(setup_lab_experiment):
    _, experiment_config = setup_lab_experiment

    return ExperimentGraph(
        experiment_config,
    )


@pytest.fixture
def clean_db(db_manager):
    print("Cleaned up DB.")
    db_manager.clean_db()


@pytest.fixture
def container_manager(setup_lab_experiment, configuration_manager, db_manager, clean_db):
    return ContainerManager(configuration_manager, db_manager)


@pytest.fixture
def device_manager(setup_lab_experiment, configuration_manager, db_manager, ray_cluster, clean_db):
    device_manager = DeviceManager(configuration_manager, db_manager)
    device_manager.update_devices(loaded_labs=set(configuration_manager.labs.keys()))
    yield device_manager
    device_manager.cleanup_device_actors()


@pytest.fixture
def experiment_manager(setup_lab_experiment, configuration_manager, db_manager, clean_db):
    return ExperimentManager(configuration_manager, db_manager)


@pytest.fixture
def container_allocator(setup_lab_experiment, configuration_manager, db_manager, clean_db):
    return ContainerAllocationManager(configuration_manager, db_manager)


@pytest.fixture
def device_allocator(setup_lab_experiment, configuration_manager, db_manager, clean_db):
    return DeviceAllocationManager(configuration_manager, db_manager)


@pytest.fixture
def resource_allocation_manager(setup_lab_experiment, configuration_manager, db_manager, clean_db):
    return ResourceAllocationManager(configuration_manager, db_manager)


@pytest.fixture
def task_manager(setup_lab_experiment, configuration_manager, db_manager, file_db_manager, clean_db):
    return TaskManager(configuration_manager, db_manager, file_db_manager)


@pytest.fixture(scope="module")
def ray_cluster():
    ray.init(namespace="test-eos", ignore_reinit_error=True, resources={"eos-core": 1})
    yield
    ray.shutdown()


@pytest.fixture
def task_executor(
    setup_lab_experiment,
    task_manager,
    device_manager,
    container_manager,
    resource_allocation_manager,
    configuration_manager,
):
    return TaskExecutor(
        task_manager, device_manager, container_manager, resource_allocation_manager, configuration_manager
    )


@pytest.fixture
def basic_scheduler(
    setup_lab_experiment,
    configuration_manager,
    experiment_manager,
    task_manager,
    device_manager,
    resource_allocation_manager,
):
    return BasicScheduler(
        configuration_manager, experiment_manager, task_manager, device_manager, resource_allocation_manager
    )


@pytest.fixture
def experiment_executor(
    request,
    experiment_manager,
    task_manager,
    container_manager,
    task_executor,
    basic_scheduler,
    experiment_graph,
):
    experiment_id, experiment_type = request.param

    return ExperimentExecutor(
        experiment_id=experiment_id,
        experiment_type=experiment_type,
        execution_parameters=ExperimentExecutionParameters(),
        experiment_graph=experiment_graph,
        experiment_manager=experiment_manager,
        task_manager=task_manager,
        container_manager=container_manager,
        task_executor=task_executor,
        scheduler=basic_scheduler,
    )


@pytest.fixture
def experiment_executor_factory(
    configuration_manager,
    experiment_manager,
    task_manager,
    container_manager,
    task_executor,
    basic_scheduler,
):
    return ExperimentExecutorFactory(
        configuration_manager=configuration_manager,
        experiment_manager=experiment_manager,
        task_manager=task_manager,
        container_manager=container_manager,
        task_executor=task_executor,
        scheduler=basic_scheduler,
    )


@pytest.fixture
def campaign_manager(
    configuration_manager,
    db_manager,
):
    return CampaignManager(configuration_manager, db_manager)


@pytest.fixture
def campaign_optimizer_manager(
    db_manager,
):
    return CampaignOptimizerManager(db_manager)


@pytest.fixture
def campaign_executor(
    request,
    configuration_manager,
    campaign_manager,
    campaign_optimizer_manager,
    task_manager,
    experiment_executor_factory,
):
    campaign_id, experiment_type, max_experiments, do_optimization = request.param

    optimizer_computer_ip = "127.0.0.1"

    execution_parameters = CampaignExecutionParameters(
        max_experiments=max_experiments,
        max_concurrent_experiments=1,
        do_optimization=do_optimization,
        optimizer_computer_ip=optimizer_computer_ip,
    )

    return CampaignExecutor(
        campaign_id=campaign_id,
        experiment_type=experiment_type,
        campaign_manager=campaign_manager,
        campaign_optimizer_manager=campaign_optimizer_manager,
        task_manager=task_manager,
        experiment_executor_factory=experiment_executor_factory,
        execution_parameters=execution_parameters,
    )
