from datetime import datetime, timedelta


def find_closest_date(target, dates):
    """
    Find the closest date to the target in the dates list.

    :param target: Target datetime.datetime object.
    :param dates: List of datetime.datetime objects.
    :return: Closest datetime.datetime object from the list.

    ---------------------
    # Example usage
    target = datetime.strptime("2023-01-15", "%Y-%m-%d")
    dates_list = [
        datetime.strptime("2020", "%Y"),
        datetime.strptime("2022", "%Y"),
        datetime.strptime("2025", "%Y"),
    ]

    print(closest_date(target, dates_list))
    """

    # If the list is empty, return None
    if not dates:
        return None

    # Sort the dates
    dates = sorted(dates)

    # Use min function with a key based on the absolute difference between the target and each date
    closest = min(dates, key=lambda date: abs(target - date))

    return closest
