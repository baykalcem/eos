import asyncio
import atexit
import traceback
from asyncio import Lock as AsyncLock
from collections.abc import AsyncIterable
from typing import Any, TYPE_CHECKING

import ray

from eos.campaigns.campaign_executor_factory import CampaignExecutorFactory
from eos.campaigns.campaign_manager import CampaignManager
from eos.campaigns.campaign_optimizer_manager import CampaignOptimizerManager
from eos.campaigns.entities.campaign import CampaignStatus, CampaignExecutionParameters, Campaign
from eos.campaigns.exceptions import EosCampaignExecutionError
from eos.configuration.configuration_manager import ConfigurationManager
from eos.configuration.entities.lab import LabDeviceConfig
from eos.configuration.entities.task import TaskConfig
from eos.configuration.entities.task_specification import TaskSpecification
from eos.configuration.exceptions import EosConfigurationError
from eos.configuration.validation import validation_utils
from eos.containers.container_manager import ContainerManager
from eos.devices.device_manager import DeviceManager
from eos.experiments.entities.experiment import ExperimentStatus, Experiment, ExperimentExecutionParameters
from eos.experiments.exceptions import EosExperimentExecutionError
from eos.experiments.experiment_executor_factory import ExperimentExecutorFactory
from eos.experiments.experiment_manager import ExperimentManager
from eos.logging.logger import log
from eos.monitoring.graceful_termination_monitor import GracefulTerminationMonitor
from eos.orchestration.exceptions import (
    EosExperimentTypeInUseError,
    EosExperimentDoesNotExistError,
    EosError,
)
from eos.persistence.db_manager import DbManager
from eos.persistence.file_db_manager import FileDbManager
from eos.persistence.service_credentials import ServiceCredentials
from eos.resource_allocation.resource_allocation_manager import (
    ResourceAllocationManager,
)
from eos.scheduling.basic_scheduler import BasicScheduler
from eos.tasks.entities.task import Task, TaskStatus
from eos.tasks.on_demand_task_executor import OnDemandTaskExecutor
from eos.tasks.task_executor import TaskExecutor
from eos.tasks.task_manager import TaskManager
from eos.utils.singleton import Singleton

if TYPE_CHECKING:
    from eos.campaigns.campaign_executor import CampaignExecutor
    from eos.experiments.experiment_executor import ExperimentExecutor


class Orchestrator(metaclass=Singleton):
    """
    The top-level orchestrator that initializes and manages all EOS components.
    """

    def __init__(
        self,
        user_dir: str,
        db_credentials: ServiceCredentials,
        file_db_credentials: ServiceCredentials,
    ):
        self._user_dir = user_dir
        self._db_credentials = db_credentials
        self._file_db_credentials = file_db_credentials
        self._initialized = False

        self.initialize()
        atexit.register(self.terminate)

    def initialize(self) -> None:
        """
        Prepare the orchestrator. This is required before any other operations can be performed.
        """
        if self._initialized:
            return

        log.info("Initializing EOS...")
        log.info("Initializing Ray cluster...")
        ray.init(namespace="eos", resources={"eos-core": 1000})
        log.info("Ray initialized.")

        # Configuration ###########################################
        self._configuration_manager = ConfigurationManager(self._user_dir)

        # Persistence #############################################
        self._db_manager = DbManager(self._db_credentials)
        self._file_db_manager = FileDbManager(self._file_db_credentials)

        # Monitoring ##############################################
        self._graceful_termination_monitor = GracefulTerminationMonitor(self._db_manager)

        # State management ########################################
        self._device_manager = DeviceManager(self._configuration_manager, self._db_manager)
        self._container_manager = ContainerManager(self._configuration_manager, self._db_manager)
        self._resource_allocation_manager = ResourceAllocationManager(self._configuration_manager, self._db_manager)
        self._task_manager = TaskManager(self._configuration_manager, self._db_manager, self._file_db_manager)
        self._experiment_manager = ExperimentManager(self._configuration_manager, self._db_manager)
        self._campaign_manager = CampaignManager(self._configuration_manager, self._db_manager)
        self._campaign_optimizer_manager = CampaignOptimizerManager(self._db_manager)

        # Execution ###############################################
        self._task_executor = TaskExecutor(
            self._task_manager,
            self._device_manager,
            self._container_manager,
            self._resource_allocation_manager,
            self._configuration_manager,
        )
        self._on_demand_task_executor = OnDemandTaskExecutor(
            self._task_executor, self._task_manager, self._container_manager
        )
        self._scheduler = BasicScheduler(
            self._configuration_manager,
            self._experiment_manager,
            self._task_manager,
            self._device_manager,
            self._resource_allocation_manager,
        )
        self._experiment_executor_factory = ExperimentExecutorFactory(
            self._configuration_manager,
            self._experiment_manager,
            self._task_manager,
            self._container_manager,
            self._task_executor,
            self._scheduler,
        )
        self._campaign_executor_factory = CampaignExecutorFactory(
            self._configuration_manager,
            self._campaign_manager,
            self._campaign_optimizer_manager,
            self._task_manager,
            self._experiment_executor_factory,
        )

        self._campaign_submission_lock = AsyncLock()
        self._submitted_campaigns: dict[str, CampaignExecutor] = {}
        self._experiment_submission_lock = AsyncLock()
        self._submitted_experiments: dict[str, ExperimentExecutor] = {}

        self._campaign_cancellation_queue = asyncio.Queue(maxsize=100)
        self._experiment_cancellation_queue = asyncio.Queue(maxsize=100)

        self._loading_lock = AsyncLock()

        self._fail_all_running_work()

        self._initialized = True

    def _fail_all_running_work(self) -> None:
        """
        When the orchestrator starts, fail all running tasks, experiments, and campaigns.
        This is for safety, as if the orchestrator was terminated while there was running work then the state of the
        system may be unknown. We want to force manual review of the state of the system and explicitly require
        re-submission of any work that was running.
        """
        running_tasks = self._task_manager.get_tasks(status=TaskStatus.RUNNING.value)
        for task in running_tasks:
            self._task_manager.fail_task(task.experiment_id, task.id)
            log.warning(f"EXP '{task.experiment_id}' - Failed task '{task.id}'.")

        running_experiments = self._experiment_manager.get_experiments(status=ExperimentStatus.RUNNING.value)
        for experiment in running_experiments:
            self._experiment_manager.fail_experiment(experiment.id)

        running_campaigns = self._campaign_manager.get_campaigns(status=CampaignStatus.RUNNING.value)
        for campaign in running_campaigns:
            self._campaign_manager.fail_campaign(campaign.id)

        if running_tasks:
            log.warning("All running tasks have been marked as failed. Please review the state of the system.")

        if running_experiments:
            log.warning(
                "All running experiments have been marked as failed. Please review the state of the system and "
                "re-submit with resume=True."
            )

        if running_campaigns:
            log.warning(
                "All running campaigns have been marked as failed. Please review the state of the system and re-submit "
                "with resume=True."
            )

    def terminate(self) -> None:
        """
        Terminate the orchestrator. After this, no other operations can be performed.
        This should be called before the program exits.
        """
        if not self._initialized:
            return
        log.info("Cleaning up device actors...")
        self._device_manager.cleanup_device_actors()
        log.info("Shutting down Ray cluster...")
        ray.shutdown()
        self._graceful_termination_monitor.terminated_gracefully()
        self._initialized = False

    def load_labs(self, labs: set[str]) -> None:
        """
        Load one or more labs into the orchestrator.
        """
        self._configuration_manager.load_labs(labs)
        self._device_manager.update_devices(loaded_labs=labs)
        self._container_manager.update_containers(loaded_labs=labs)

    def unload_labs(self, labs: set[str]) -> None:
        """
        Unload one or more labs from the orchestrator.
        """
        self._configuration_manager.unload_labs(labs)
        self._device_manager.update_devices(unloaded_labs=labs)
        self._container_manager.update_containers(unloaded_labs=labs)

    async def reload_labs(self, lab_types: set[str]) -> None:
        """
        Reload one or more labs in the orchestrator.
        """
        async with self._loading_lock:
            experiments_to_reload = set()
            for lab_type in lab_types:
                existing_experiments = self._experiment_manager.get_experiments(status=ExperimentStatus.RUNNING.value)

                for experiment in existing_experiments:
                    experiment_config = self._configuration_manager.experiments[experiment.type]
                    if lab_type in experiment_config.labs:
                        raise EosExperimentTypeInUseError(
                            f"Cannot reload lab type '{lab_type}' as there are running experiments that use it."
                        )

                # Determine experiments to reload for this lab type
                for experiment_type, experiment_config in self._configuration_manager.experiments.items():
                    if lab_type in experiment_config.labs:
                        experiments_to_reload.add(experiment_type)
            try:
                self.unload_labs(lab_types)
                self.load_labs(lab_types)
                self.load_experiments(experiments_to_reload)
            except EosConfigurationError:
                log.error(f"Error reloading labs: {traceback.format_exc()}")
                raise

    async def update_loaded_labs(self, lab_types: set[str]) -> None:
        """
        Update the loaded labs with new configurations.
        """
        async with self._loading_lock:
            currently_loaded = set(self._configuration_manager.labs.keys())

            if currently_loaded == lab_types:
                return

            to_unload = currently_loaded - lab_types
            to_load = lab_types - currently_loaded

            for lab_type in to_unload:
                existing_experiments = self._experiment_manager.get_experiments(status=ExperimentStatus.RUNNING.value)

                for experiment in existing_experiments:
                    experiment_config = self._configuration_manager.experiments[experiment.type]
                    if lab_type in experiment_config.labs:
                        raise EosExperimentTypeInUseError(
                            f"Cannot unload lab type '{lab_type}' as there are running experiments that use it."
                        )

            try:
                self.unload_labs(to_unload)
                self.load_labs(to_load)
            except EosConfigurationError:
                log.error(f"Error updating loaded labs: {traceback.format_exc()}")
                raise

    async def get_lab_loaded_statuses(self) -> dict[str, bool]:
        """
        Return a dictionary of lab types and a boolean indicating whether they are loaded.
        """
        return self._configuration_manager.get_lab_loaded_statuses()

    def load_experiments(self, experiment_types: set[str]) -> None:
        """
        Load one or more experiments into the orchestrator.
        """
        self._configuration_manager.load_experiments(experiment_types)

    def unload_experiments(self, experiment_types: set[str]) -> None:
        """
        Unload one or more experiments from the orchestrator.
        """
        self._configuration_manager.unload_experiments(experiment_types)

    async def reload_experiments(self, experiment_types: set[str]) -> None:
        """
        Reload one or more experiments in the orchestrator.
        """
        async with self._loading_lock:
            for experiment_type in experiment_types:
                existing_experiments = self._experiment_manager.get_experiments(
                    status=ExperimentStatus.RUNNING.value, type=experiment_type
                )
                if existing_experiments:
                    raise EosExperimentTypeInUseError(
                        f"Cannot reload experiment type '{experiment_type}' as there are running experiments of this "
                        f"type."
                    )
            try:
                self.unload_experiments(experiment_types)
                self.load_experiments(experiment_types)
            except EosConfigurationError:
                log.error(f"Error reloading experiments: {traceback.format_exc()}")
                raise

    async def update_loaded_experiments(self, experiment_types: set[str]) -> None:
        """
        Update the loaded experiments with new configurations.
        """
        async with self._loading_lock:
            currently_loaded = set(self._configuration_manager.experiments.keys())

            if currently_loaded == experiment_types:
                return

            to_unload = currently_loaded - experiment_types
            to_load = experiment_types - currently_loaded

            for experiment_type in to_unload:
                existing_experiments = self._experiment_manager.get_experiments(
                    status=ExperimentStatus.RUNNING.value, type=experiment_type
                )
                if existing_experiments:
                    raise EosExperimentTypeInUseError(
                        f"Cannot unload experiment type '{experiment_type}' as there are running experiments of this "
                        f"type."
                    )

            try:
                self.unload_experiments(to_unload)
                self.load_experiments(to_load)
            except EosConfigurationError:
                log.error(f"Error updating loaded experiments: {traceback.format_exc()}")
                raise

    async def get_experiment_loaded_statuses(self) -> dict[str, bool]:
        """
        Return a dictionary of experiment types and a boolean indicating whether they are loaded.
        """
        return self._configuration_manager.get_experiment_loaded_statuses()

    async def get_lab_devices(
        self, lab_types: set[str] | None = None, task_type: str | None = None
    ) -> dict[str, dict[str, LabDeviceConfig]]:
        """
        Get the devices that are available in the given labs or for a specific task type.

        :param lab_types: The lab types. If None, all labs will be considered.
        :param task_type: The task type. If provided, only devices supporting this task type will be returned.
        :return: A dictionary of lab types and the devices available in each lab.
        """
        lab_devices = {}

        if not lab_types or not any(lab_type.strip() for lab_type in lab_types):
            lab_types = set(self._configuration_manager.labs.keys())

        task_device_types = set()
        if task_type:
            task_spec = self._configuration_manager.task_specs.get_spec_by_type(task_type)
            task_device_types = set(task_spec.device_types) if task_spec.device_types else set()

        for lab_type in lab_types:
            lab = self._configuration_manager.labs.get(lab_type)
            if not lab:
                continue

            if task_device_types:
                devices = {name: device for name, device in lab.devices.items() if device.type in task_device_types}
            else:
                devices = lab.devices

            if devices:
                lab_devices[lab_type] = devices

        return lab_devices

    async def get_task(self, experiment_id: str, task_id: str) -> Task:
        """
        Get a task by its unique identifier.

        :param experiment_id: The unique identifier of the experiment.
        :param task_id: The unique identifier of the task.
        :return: The task entity.
        """
        return self._task_manager.get_task(experiment_id, task_id)

    async def submit_task(
        self,
        task_config: TaskConfig,
        resource_allocation_priority: int = 1,
        resource_allocation_timeout: int = 180,
    ) -> None:
        """
        Submit a new task for execution. By default, tasks submitted in this way have maximum resource allocation
        priority and a timeout of 180 seconds.

        :param task_config: The task configuration. This is the same data as defined in an experiment configuration.
        :param resource_allocation_priority: The priority of the task in acquiring resources.
        :param resource_allocation_timeout: The maximum seconds to wait for resources to be allocated before raising an
        error.
        :return: The output of the task.
        """
        await self._on_demand_task_executor.submit_task(
            task_config, resource_allocation_priority, resource_allocation_timeout
        )

    async def cancel_task(self, task_id: str, experiment_id: str = "on_demand") -> None:
        """
        Cancel a task that is currently being executed.

        :param task_id: The unique identifier of the task.
        :param experiment_id: The unique identifier of the experiment.
        """
        if experiment_id == "on_demand":
            await self._on_demand_task_executor.cancel_task(task_id)
        else:
            await self._task_executor.request_task_cancellation(experiment_id, task_id)

    async def get_task_types(self) -> list[str]:
        """
        Get a list of all task types that are defined in the configuration.
        """
        return [task.type for task in self._configuration_manager.task_specs.get_all_specs().values()]

    async def get_task_spec(self, task_type: str) -> TaskSpecification | None:
        """
        Get the task specification for a given task type.
        """
        task_spec = self._configuration_manager.task_specs.get_spec_by_type(task_type)
        if not task_spec:
            raise EosError(f"Task type '{task_type}' does not exist.")

        return task_spec

    def stream_task_output_file(
        self, experiment_id: str, task_id: str, file_name: str, chunk_size: int = 3 * 1024 * 1024
    ) -> AsyncIterable[bytes]:
        """
        Stream the contents of a task output file in chunks.
        """
        return self._task_manager.stream_task_output_file(experiment_id, task_id, file_name, chunk_size)

    async def list_task_output_files(self, experiment_id: str, task_id: str) -> list[str]:
        """
        Get a list of all output files for a given task.
        """
        return self._task_manager.list_task_output_files(experiment_id, task_id)

    async def get_experiment(self, experiment_id: str) -> Experiment | None:
        """
        Get an experiment by its unique identifier.

        :param experiment_id: The unique identifier of the experiment.
        :return: The experiment entity.
        """
        return self._experiment_manager.get_experiment(experiment_id)

    async def submit_experiment(
        self,
        experiment_id: str,
        experiment_type: str,
        execution_parameters: ExperimentExecutionParameters,
        dynamic_parameters: dict[str, dict[str, Any]],
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """
        Submit a new experiment for execution. The experiment will be executed asynchronously.

        :param experiment_id: The unique identifier of the experiment.
        :param experiment_type: The type of the experiment. Must have a configuration defined in the
        configuration manager.
        :param execution_parameters: The execution parameters for the experiment.
        :param dynamic_parameters: The dynamic parameters for the experiment.
        :param metadata: Any additional metadata.
        """
        self._validate_experiment_type_exists(experiment_type)

        async with self._experiment_submission_lock:
            if experiment_id in self._submitted_experiments:
                log.warning(f"Experiment '{experiment_id}' is already submitted. Ignoring new submission.")
                return

            experiment_executor = self._experiment_executor_factory.create(
                experiment_id, experiment_type, execution_parameters
            )

            try:
                experiment_executor.start_experiment(dynamic_parameters, metadata)
                self._submitted_experiments[experiment_id] = experiment_executor
            except EosExperimentExecutionError:
                log.error(f"Failed to submit experiment '{experiment_id}': {traceback.format_exc()}")
                del self._submitted_experiments[experiment_id]
                return

            log.info(f"Submitted experiment '{experiment_id}'.")

    async def cancel_experiment(self, experiment_id: str) -> None:
        """
        Cancel an experiment that is currently being executed.

        :param experiment_id: The unique identifier of the experiment.
        """
        if experiment_id in self._submitted_experiments:
            await self._experiment_cancellation_queue.put(experiment_id)

    async def get_experiment_types(self) -> list[str]:
        """
        Get a list of all experiment types that are defined in the configuration.
        """
        return list(self._configuration_manager.experiments.keys())

    async def get_experiment_dynamic_params_template(self, experiment_type: str) -> dict[str, Any]:
        """
        Get the dynamic parameters template for a given experiment type.

        :param experiment_type: The type of the experiment.
        :return: The dynamic parameter template.
        """
        experiment_config = self._configuration_manager.experiments[experiment_type]
        dynamic_parameters = {}

        for task in experiment_config.tasks:
            task_dynamic_parameters = {}
            for parameter_name, parameter_value in task.parameters.items():
                if validation_utils.is_dynamic_parameter(parameter_value):
                    task_dynamic_parameters[parameter_name] = "PLACEHOLDER"
            if task_dynamic_parameters:
                dynamic_parameters[task.id] = task_dynamic_parameters

        return dynamic_parameters

    async def get_campaign(self, campaign_id: str) -> Campaign | None:
        """
        Get a campaign by its unique identifier.

        :param campaign_id: The unique identifier of the campaign.
        :return: The campaign entity.
        """
        return self._campaign_manager.get_campaign(campaign_id)

    async def submit_campaign(
        self,
        campaign_id: str,
        experiment_type: str,
        campaign_execution_parameters: CampaignExecutionParameters,
    ) -> None:
        self._validate_experiment_type_exists(experiment_type)

        async with self._campaign_submission_lock:
            if campaign_id in self._submitted_campaigns:
                log.warning(f"Campaign '{campaign_id}' is already submitted. Ignoring new submission.")
                return

            campaign_executor = self._campaign_executor_factory.create(
                campaign_id, experiment_type, campaign_execution_parameters
            )

            try:
                await campaign_executor.start_campaign()
                self._submitted_campaigns[campaign_id] = campaign_executor
            except EosCampaignExecutionError:
                log.error(f"Failed to submit campaign '{campaign_id}': {traceback.format_exc()}")
                del self._submitted_campaigns[campaign_id]
                return

            log.info(f"Submitted campaign '{campaign_id}'.")

    async def cancel_campaign(self, campaign_id: str) -> None:
        """
        Cancel a campaign that is currently being executed.

        :param campaign_id: The unique identifier of the campaign.
        """
        if campaign_id in self._submitted_campaigns:
            await self._campaign_cancellation_queue.put(campaign_id)

    async def spin(self, rate_hz: int = 10) -> None:
        """
        Spin the orchestrator at a given rate in Hz.

        :param rate_hz: The processing rate in Hz. This is the rate in which the orchestrator will check for progress in
         submitted experiments and campaigns.
        """
        while True:
            await self._process_experiment_and_campaign_cancellations()

            await asyncio.gather(
                self._process_on_demand_tasks(),
                self._process_experiments(),
                self._process_campaigns(),
            )
            self._resource_allocation_manager.process_active_requests()

            await asyncio.sleep(1 / rate_hz)

    async def _process_experiment_and_campaign_cancellations(self) -> None:
        while not self._experiment_cancellation_queue.empty():
            experiment_id = await self._experiment_cancellation_queue.get()

            log.warning(f"Attempting to cancel experiment '{experiment_id}'.")
            try:
                await self._submitted_experiments[experiment_id].cancel_experiment()
            finally:
                del self._submitted_experiments[experiment_id]
            log.warning(f"Cancelled experiment '{experiment_id}'.")

        while not self._campaign_cancellation_queue.empty():
            campaign_id = await self._campaign_cancellation_queue.get()

            log.warning(f"Attempting to cancel campaign '{campaign_id}'.")
            try:
                await self._submitted_campaigns[campaign_id].cancel_campaign()
            finally:
                self._submitted_campaigns[campaign_id].cleanup()
                del self._submitted_campaigns[campaign_id]
            log.warning(f"Cancelled campaign '{campaign_id}'.")

    async def _process_experiments(self) -> None:
        to_remove_completed = []
        to_remove_failed = []

        for experiment_id, experiment_executor in self._submitted_experiments.items():
            try:
                completed = await experiment_executor.progress_experiment()

                if completed:
                    to_remove_completed.append(experiment_id)
            except EosExperimentExecutionError:
                log.error(f"Error in experiment '{experiment_id}': {traceback.format_exc()}")
                to_remove_failed.append(experiment_id)

        for experiment_id in to_remove_completed:
            log.info(f"Completed experiment '{experiment_id}'.")
            del self._submitted_experiments[experiment_id]

        for experiment_id in to_remove_failed:
            log.error(f"Failed experiment '{experiment_id}'.")
            del self._submitted_experiments[experiment_id]

    async def _process_campaigns(self) -> None:
        async def process_single_campaign(campaign_id: str, campaign_executor) -> tuple[str, bool, bool]:
            try:
                completed = await campaign_executor.progress_campaign()
                return campaign_id, completed, False
            except EosCampaignExecutionError:
                log.error(f"Error in campaign '{campaign_id}': {traceback.format_exc()}")
                return campaign_id, False, True

        results = await asyncio.gather(
            *(process_single_campaign(cid, executor) for cid, executor in self._submitted_campaigns.items()),
        )

        to_remove_completed: list[str] = []
        to_remove_failed: list[str] = []

        for campaign_id, completed, failed in results:
            if completed:
                to_remove_completed.append(campaign_id)
            elif failed:
                to_remove_failed.append(campaign_id)

        for campaign_id in to_remove_completed:
            log.info(f"Completed campaign '{campaign_id}'.")
            self._submitted_campaigns[campaign_id].cleanup()
            del self._submitted_campaigns[campaign_id]

        for campaign_id in to_remove_failed:
            log.error(f"Failed campaign '{campaign_id}'.")
            self._submitted_campaigns[campaign_id].cleanup()
            del self._submitted_campaigns[campaign_id]

    async def _process_on_demand_tasks(self) -> None:
        await self._on_demand_task_executor.process_tasks()

    def _validate_experiment_type_exists(self, experiment_type: str) -> None:
        if experiment_type not in self._configuration_manager.experiments:
            raise EosExperimentDoesNotExistError(
                f"Cannot submit experiment of type '{experiment_type}' as it does not exist."
            )

    @property
    def configuration_manager(self) -> ConfigurationManager:
        return self._configuration_manager

    @property
    def db_manager(self) -> DbManager:
        return self._db_manager

    @property
    def device_manager(self) -> DeviceManager:
        return self._device_manager

    @property
    def container_manager(self) -> ContainerManager:
        return self._container_manager

    @property
    def resource_allocation_manager(self) -> ResourceAllocationManager:
        return self._resource_allocation_manager

    @property
    def task_manager(self) -> TaskManager:
        return self._task_manager

    @property
    def experiment_manager(self) -> ExperimentManager:
        return self._experiment_manager

    @property
    def campaign_manager(self) -> CampaignManager:
        return self._campaign_manager

    @property
    def task_executor(self) -> TaskExecutor:
        return self._task_executor
