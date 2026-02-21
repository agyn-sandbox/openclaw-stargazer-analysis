from src.metrics import MetricsRunner
from src.utils import utc_now


def test_utc_now_returns_timezone_aware_datetime() -> None:
    value = utc_now()
    assert value.tzinfo is not None


def test_score_to_label_thresholds() -> None:
    runner = MetricsRunner.__new__(MetricsRunner)  # bypass __init__
    assert runner._score_to_label(75) == "likely_bot"
    assert runner._score_to_label(45) == "suspicious"
    assert runner._score_to_label(10) == "likely_human"
