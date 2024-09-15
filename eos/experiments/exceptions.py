class EosExperimentError(Exception):
    pass


class EosExperimentStateError(EosExperimentError):
    pass


class EosExperimentTaskExecutionError(EosExperimentError):
    pass


class EosExperimentExecutionError(EosExperimentError):
    pass


class EosExperimentCancellationError(EosExperimentError):
    pass
