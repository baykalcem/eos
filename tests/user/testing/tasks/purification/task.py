from eos.tasks.base_task import BaseTask


class PurificationTask(BaseTask):
    def _execute(
        self,
        devices: BaseTask.DevicesType,
        parameters: BaseTask.ParametersType,
        containers: BaseTask.ContainersType,
    ) -> BaseTask.OutputType:
        output_parameters = {"water_salinity": 0.02}

        return output_parameters, None, None
