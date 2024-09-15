class EosExperimentTypeInUseError(Exception):
    pass


class EosFailedExperimentRecoveryError(Exception):
    pass


class EosFailedCampaignRecoveryError(Exception):
    pass


class EosExperimentDoesNotExistError(Exception):
    pass


class EosError(Exception):
    pass
