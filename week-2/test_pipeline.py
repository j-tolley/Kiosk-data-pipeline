# pylint: disable= W0612

from datetime import datetime
from pipeline import (check_valid_time, check_at_key, check_site_key,
                      check_val_key, check_type_key, check_message)


# ===== check_valid_time() tests =====

def test_check_valid_time_valid_times():
    """Test valid times within the 8:45 AM - 6:15 PM range."""
    assert check_valid_time(datetime.fromisoformat(
        "2024-01-01T10:00:00+00:00")) is True
    assert check_valid_time(datetime.fromisoformat(
        "2024-01-01T16:00:00+00:00")) is True
    assert check_valid_time(datetime.fromisoformat(
        "2024-01-01T08:45:00+00:00")) is True
    assert check_valid_time(datetime.fromisoformat(
        "2024-01-01T18:14:00+00:00")) is True


def test_check_valid_time_before_day():
    """Test times before 8:45 AM."""
    assert check_valid_time(datetime.fromisoformat(
        "2024-01-01T08:44:00+00:00")) is False
    assert check_valid_time(datetime.fromisoformat(
        "2024-01-01T07:00:00+00:00")) is False
    assert check_valid_time(datetime.fromisoformat(
        "2024-01-01T00:00:00+00:00")) is False


def test_check_valid_time_after_day():
    """Test times after 6:15 PM."""
    assert check_valid_time(datetime.fromisoformat(
        "2024-01-01T18:15:00+00:00")) is False
    assert check_valid_time(datetime.fromisoformat(
        "2024-01-01T19:00:00+00:00")) is False
    assert check_valid_time(datetime.fromisoformat(
        "2024-01-01T23:59:00+00:00")) is False


def test_check_valid_time_invalid_type_int():
    """Test with integer input (timestamp)."""
    assert check_valid_time(1704110400) is False


def test_check_valid_time_invalid_type_string():
    """Test with string input."""
    assert check_valid_time("2024-01-01T10:00:00") is False


def test_check_valid_time_invalid_type_none():
    """Test with None input."""
    assert check_valid_time(None) is False


def test_check_valid_time_invalid_type_float():
    """Test with float input."""
    assert check_valid_time(123.456) is False


def test_check_valid_time_invalid_type_list():
    """Test with list input."""
    assert check_valid_time([2024, 1, 1, 10, 0, 0]) is False


def test_check_valid_time_invalid_type_dict():
    """Test with dict input."""
    assert check_valid_time({"hour": 10, "minute": 0}) is False


# ===== check_at_key() tests =====

def test_check_at_key_valid_datetime():
    """Test valid ISO format datetime strings."""
    msg = {"at": "2024-01-01T10:00:00"}
    assert check_at_key(msg) is True
    assert isinstance(msg["at"], datetime)


def test_check_at_key_valid_datetime_with_timezone():
    """Test valid ISO format with timezone."""
    msg = {"at": "2024-01-01T10:00:00+00:00"}
    assert check_at_key(msg) is True
    assert isinstance(msg["at"], datetime)


def test_check_at_key_invalid_format():
    """Test invalid datetime format."""
    msg = {"at": "2024/01/01 10:00"}
    assert check_at_key(msg) is False


def test_check_at_key_missing_key():
    """Test missing 'at' key."""
    msg = {"other": "value"}
    assert check_at_key(msg) is False


def test_check_at_key_invalid_time():
    """Test valid format but invalid time range."""
    msg = {"at": "2024-01-01T07:00:00"}
    assert check_at_key(msg) is False


# ===== check_site_key() tests =====

def test_check_site_key_valid_int():
    """Test valid site values as integers."""
    msg = {"site": 0}
    assert check_site_key(msg) is True
    msg = {"site": 5}
    assert check_site_key(msg) is True
    msg = {"site": 3}
    assert check_site_key(msg) is True


def test_check_site_key_valid_numeric_string():
    """Test valid site values as numeric strings."""
    msg = {"site": "0"}
    assert check_site_key(msg) is True
    assert msg["site"] == 0
    msg = {"site": "5"}
    assert check_site_key(msg) is True
    assert msg["site"] == 5


def test_check_site_key_invalid_range():
    """Test site values outside valid range."""
    msg = {"site": -1}
    assert check_site_key(msg) is False
    msg = {"site": 6}
    assert check_site_key(msg) is False
    msg = {"site": 10}
    assert check_site_key(msg) is False


def test_check_site_key_invalid_string():
    """Test invalid string site values."""
    msg = {"site": "abc"}
    assert check_site_key(msg) is False
    msg = {"site": "1.5"}
    assert check_site_key(msg) is False


def test_check_site_key_missing_key():
    """Test missing 'site' key."""
    msg = {"other": "value"}
    assert check_site_key(msg) is False


# ===== check_val_key() tests =====

def test_check_val_key_valid_values():
    """Test valid val values."""
    for val in (-1, 0, 1, 2, 3, 4):
        msg = {"val": val}
        assert check_val_key(msg) is True


def test_check_val_key_invalid_values():
    """Test invalid val values."""
    for val in (-2, -5, 5, 10, 100):
        msg = {"val": val}
        assert check_val_key(msg) is False


def test_check_val_key_missing_key():
    """Test missing 'val' key."""
    msg = {"other": "value"}
    assert check_val_key(msg) is False


# ===== check_type_key() tests =====

def test_check_type_key_valid_values():
    """Test valid type values."""
    msg = {"type": 0}
    assert check_type_key(msg) is True
    msg = {"type": 1}
    assert check_type_key(msg) is True


def test_check_type_key_invalid_values():
    """Test invalid type values."""
    msg = {"type": 2}
    assert check_type_key(msg) is False
    msg = {"type": -1}
    assert check_type_key(msg) is False
    msg = {"type": 5}
    assert check_type_key(msg) is False


def test_check_type_key_missing_key():
    """Test missing 'type' key."""
    msg = {"other": "value"}
    assert check_type_key(msg) is False


# ===== check_message() tests =====

def test_check_message_valid_with_type():
    """Test valid message with val=-1 and valid type."""
    msg = {
        "at": "2024-01-01T10:00:00",
        "site": 1,
        "val": -1,
        "type": 0
    }
    assert check_message(msg) is True


def test_check_message_valid_without_type():
    """Test valid message with val!=−1, type should be set to None."""
    msg = {
        "at": "2024-01-01T10:00:00",
        "site": 1,
        "val": 2,
        "type": 0
    }
    assert check_message(msg) is True
    assert msg["type"] is None


def test_check_message_invalid_at():
    """Test message with invalid 'at' value."""
    msg = {
        "at": "2024-01-01T07:00:00",
        "site": 1,
        "val": 1,
        "type": 0
    }
    assert check_message(msg) is False


def test_check_message_invalid_site():
    """Test message with invalid site."""
    msg = {
        "at": "2024-01-01T10:00:00",
        "site": 10,
        "val": 1,
        "type": 0
    }
    assert check_message(msg) is False


def test_check_message_invalid_val():
    """Test message with invalid val."""
    msg = {
        "at": "2024-01-01T10:00:00",
        "site": 1,
        "val": 5,
        "type": 0
    }
    assert check_message(msg) is False


def test_check_message_val_neg_without_type():
    """Test message with val=-1 but missing type."""
    msg = {
        "at": "2024-01-01T10:00:00",
        "site": 1,
        "val": -1,
    }
    assert check_message(msg) is False


def test_check_message_val_neg_invalid_type():
    """Test message with val=-1 and invalid type."""
    msg = {
        "at": "2024-01-01T10:00:00",
        "site": 1,
        "val": -1,
        "type": 2
    }
    assert check_message(msg) is False


def test_check_message_missing_required_field():
    """Test message with missing required field."""
    msg = {
        "site": 1,
        "val": 1,
        "type": 0
    }
    assert check_message(msg) is False
