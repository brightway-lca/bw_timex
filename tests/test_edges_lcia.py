"""Tests for the `edges` integration (TimexLCA.edges_lcia)."""

import importlib
import sys

import pytest
from pydantic import ValidationError


def test_missing_edges_gives_actionable_import_error(monkeypatch):
    """Without `edges` installed, importing the adapter names the extra and the Python ceiling."""
    monkeypatch.setitem(sys.modules, "edges", None)
    monkeypatch.delitem(sys.modules, "bw_timex.edges_lcia", raising=False)

    with pytest.raises(ImportError) as exc_info:
        importlib.import_module("bw_timex.edges_lcia")

    message = str(exc_info.value)
    assert "bw_timex[edges]" in message
    assert "3.13" in message


def test_edges_lcia_inputs_defaults():
    from bw_timex.validation import EdgesLCIAInputs

    inputs = EdgesLCIAInputs(method=("some", "method"))

    assert inputs.weight == "population"
    assert inputs.regionalized is True
    assert inputs.use_disaggregated_lci is False
    assert inputs.parameters is None


def test_edges_lcia_inputs_rejects_empty_method():
    from bw_timex.validation import EdgesLCIAInputs

    with pytest.raises(ValidationError):
        EdgesLCIAInputs(method=())


def test_edges_lcia_inputs_rejects_dict_without_exchanges():
    from bw_timex.validation import EdgesLCIAInputs

    with pytest.raises(ValidationError):
        EdgesLCIAInputs(method={"name": "no exchanges here"})
