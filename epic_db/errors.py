class BaseEpicDbError(Exception):
    """The base error inherited by all EpicDB errors."""


class RowNotFoundError(BaseEpicDbError):
    """The row requested by primary keys was not returned. This means the row
    does not exist but all composite keys are present and the row should be
    created."""


class SequelaSetVersionValidationError(BaseEpicDbError):
    """The sequela_set_version in question failed the validations necessary
    for activation"""
