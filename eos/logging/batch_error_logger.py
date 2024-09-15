class BatchErrorLogger:
    """
    The BatchErrorLogger class is used to batch-log errors together. Instead of printing
    errors as they occur, they are stored in a list and can be printed all at once.
    """

    def __init__(self):
        self.errors: list[tuple[str, type[Exception]]] = []

    def batch_error(self, message: str, exception_type: type[Exception]) -> None:
        self.errors.append((message, exception_type))

    def raise_batched_errors(self, root_exception_type: type[Exception] = Exception) -> None:
        if self.errors:
            error_messages = "\n\n".join(
                f"{message} ({exception_type.__name__})" for message, exception_type in self.errors
            )
            self.errors.clear()
            raise root_exception_type(error_messages)


def batch_error(message: str, exception_type: type[Exception]) -> None:
    batch_logger.batch_error(message, exception_type)


def raise_batched_errors(root_exception_type: type[Exception] = Exception) -> None:
    batch_logger.raise_batched_errors(root_exception_type)


batch_logger = BatchErrorLogger()
