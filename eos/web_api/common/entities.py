from typing import Any

from pydantic import BaseModel

from eos.campaigns.entities.campaign import CampaignExecutionParameters
from eos.configuration.entities.task import TaskConfig
from eos.experiments.entities.experiment import ExperimentExecutionParameters


class SubmitTaskRequest(BaseModel):
    task_config: TaskConfig
    resource_allocation_priority: int = 1
    resource_allocation_timeout: int = 180


class TaskTypesResponse(BaseModel):
    task_types: list[str] | str


class SubmitExperimentRequest(BaseModel):
    experiment_id: str
    experiment_type: str
    experiment_execution_parameters: ExperimentExecutionParameters
    dynamic_parameters: dict[str, dict[str, Any]]
    metadata: dict[str, Any] = {}


class ExperimentTypes(BaseModel):
    experiment_types: list[str] | str


class ExperimentLoadedStatusesResponse(BaseModel):
    experiment_loaded_statuses: dict[str, bool]


class ExperimentTypesResponse(BaseModel):
    experiment_types: list[str]


class ExperimentDynamicParamsTemplateResponse(BaseModel):
    dynamic_params_template: str


class SubmitCampaignRequest(BaseModel):
    campaign_id: str
    experiment_type: str
    campaign_execution_parameters: CampaignExecutionParameters


class LabTypes(BaseModel):
    lab_types: list[str] | str


class LabLoadedStatusesResponse(BaseModel):
    lab_loaded_statuses: dict[str, bool]
