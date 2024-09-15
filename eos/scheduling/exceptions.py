class EosSchedulerError(Exception):
    pass


class EosSchedulerRegistrationError(EosSchedulerError):
    pass


class EosSchedulerResourceAllocationError(EosSchedulerError):
    pass
