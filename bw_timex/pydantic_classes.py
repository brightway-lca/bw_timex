from datetime import datetime
from typing import Dict, Tuple, Union

import bw2data as bd
from pydantic import BaseModel, model_validator, validator


class TimexConfig(BaseModel):
    """Represents all the inputs needed to initialize Timex."""

    general: GeneralSettings


class DatabaseDateEntry(BaseModel):
    """Represents one database entry with either a datetime or 'dynamic'."""

    db_name: str
    date: Union[datetime, str]

    @validator("date")
    def date_must_be_datetime_or_dynamic(cls, value):
        if isinstance(value, datetime) or value == "dynamic":
            return value
        raise ValueError("Date must be a datetime or 'dynamic'.")


class TimexLCAConfig(BaseModel):
    """Represents all the inputs needed to initialize TimexLCA."""

    demand: Dict
    method: Tuple[str, ...]
    database_date_list: Tuple[DatabaseDateEntry, ...]

    @model_validator(mode="after")
    def check_databases_in_current_project(cls, values):
        """Ensure each database_name is present in the current Brightway project."""
        for entry in values.get("database_date_list", ()):
            if entry.db_name not in bd.databases:
                raise ValueError(
                    f"Database '{entry.db_name}' not found in current project."
                )
        return values
