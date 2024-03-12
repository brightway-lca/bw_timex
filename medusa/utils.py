import warnings
from datetime import datetime
from typing import Optional


def extract_date_as_integer(dt_obj: datetime, time_res: Optional[str] = "year") -> int:
    """
    Converts a datetime object to an integer for a given temporal resolution (time_res)

    :param dt_obj: Datetime object.
    :time_res: time resolution to be returned: year=YYYY, month=YYYYMM, day=YYYYMMDD, hour=YYYYMMDDHH
    :return: integer in the format of time_res

    """
    time_res_dict = {
        "year": "%Y",
        "month": "%Y%m",
        "day": "%Y%m%d",
        "hour": "%Y%m%d%M",
    }

    if time_res not in time_res_dict.keys():
        warnings.warn(
            'time_res: {} is not a valid option. Please choose from: {} defaulting to "year"'.format(
                time_res, time_res_dict.keys()
            ),
            category=Warning,
        )
    formatted_date = dt_obj.strftime(time_res_dict[time_res])
    date_as_integer = int(formatted_date)

    return date_as_integer
