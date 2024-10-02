class EosDeviceError(Exception):
    pass


class EosDeviceStateError(EosDeviceError):
    pass


class EosDeviceInitializationError(EosDeviceError):
    pass


class EosDeviceCleanupError(EosDeviceError):
    pass
