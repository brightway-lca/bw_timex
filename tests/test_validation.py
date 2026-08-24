import pytest

from bw_timex.validation import TimexLCAInputs


def test_method_may_be_missing_when_create_missing():
    inputs = TimexLCAInputs(
        demand={("foreground", "activity"): 1},
        method=("missing", "method"),
        create_missing=True,
        scenario={
            "iam_model": "remind",
            "pathway": "SSP2-PkBudg500",
            "system_model": "cutoff",
            "ecoinvent_version": "3.10.1",
            "years": [2030],
        },
    )

    assert inputs.method == ("missing", "method")


def test_missing_method_still_raises_without_create_missing():
    with pytest.raises(ValueError, match="Method .* not found"):
        TimexLCAInputs(
            demand={("foreground", "activity"): 1},
            method=("missing", "method"),
        )
