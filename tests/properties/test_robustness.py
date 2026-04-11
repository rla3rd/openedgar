import pytest
from hypothesis import given, strategies as st
from openedgar.parsers.openedgar import parse_filing

@given(st.binary(min_size=0, max_size=5000))
def test_parse_filing_robustness(data):
    """
    Property: parse_filing should never crash, regardless of input bytes.
    """
    try:
        result = parse_filing(data)
        assert isinstance(result, dict)
    except Exception as e:
        pytest.fail(f"parse_filing crashed with input {data!r}: {e}")

@given(st.text(min_size=0, max_size=5000))
def test_parse_filing_text_robustness(text):
    """
    Property: parse_filing should never crash on arbitrary text strings.
    """
    result = parse_filing(text)
    assert isinstance(result, dict)
