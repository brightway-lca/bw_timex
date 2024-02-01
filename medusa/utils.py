import warnings
from datetime import datetime
from typing import Optional

def extract_date_as_integer(
            dt_obj: datetime, time_res: Optional[str] = "year"
        ) -> int:
            """
            Converts a datetime object to an integer in the format YYYY
            #FIXME: ideally we want to add YYYYMMDDHH to the ids, but this cretaes integers that are too long for 32-bit C long

            :param dt_obj: Datetime object.
            :time_res: time resolution to be returned: year=YYYY, month=YYYYMM, day=YYYYMMDD, hour=YYYYMMDDHH
            :return: INTEGER in the format YYYY.

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