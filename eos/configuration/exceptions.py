class EosConfigurationError(Exception):
    pass


class EosMissingConfigurationError(Exception):
    pass


class EosExperimentConfigurationError(Exception):
    pass


class EosLabConfigurationError(Exception):
    pass


class EosContainerConfigurationError(Exception):
    pass


class EosTaskValidationError(Exception):
    pass


class EosDynamicParameterConfigurationError(Exception):
    pass


class EosTaskGraphError(Exception):
    pass


class EosTaskHandlerClassNotFoundError(Exception):
    pass


class EosCampaignOptimizerNotFoundError(Exception):
    pass
