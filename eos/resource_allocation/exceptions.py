class EosResourceRequestError(Exception):
    pass


class EosDeviceAllocatedError(EosResourceRequestError):
    pass


class EosDeviceNotFoundError(EosResourceRequestError):
    pass


class EosContainerAllocatedError(EosResourceRequestError):
    pass


class EosContainerNotFoundError(EosResourceRequestError):
    pass
