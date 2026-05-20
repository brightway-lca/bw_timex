from bw_timex.utils import convert_date_string_to_datetime


def test_convert_date_string_to_datetime_is_memoized():
    # The same (temporal_grouping, date_string) pair is parsed tens of
    # thousands of times during build_dynamic_biosphere_matrix; memoizing
    # avoids redundant strptime calls.
    convert_date_string_to_datetime.cache_clear()
    for _ in range(10):
        convert_date_string_to_datetime("year", "2024")
    info = convert_date_string_to_datetime.cache_info()
    assert info.misses == 1
    assert info.hits == 9


def test_convert_date_string_to_datetime_distinct_keys_miss():
    convert_date_string_to_datetime.cache_clear()
    convert_date_string_to_datetime("year", "2024")
    convert_date_string_to_datetime("year", "2025")
    convert_date_string_to_datetime("month", "202401")
    info = convert_date_string_to_datetime.cache_info()
    assert info.misses == 3
    assert info.hits == 0
