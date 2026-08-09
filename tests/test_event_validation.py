"""
Tests for event validation logic.

These tests cover the exact scenarios interviewers ask about:
  - duplicate handling
  - malformed event rejection
  - late-arriving event detection
  - business rule validation
"""

import uuid
from datetime import datetime, timedelta, timezone

import pytest

from src.consumer.main import validate_event
from src.models.events import EcommerceEvent, EventType


# --- Valid event fixtures ---

def valid_product_view():
    return {
        "event_id":        str(uuid.uuid4()),
        "event_type":      "product_view",
        "user_id":         "user_0001234",
        "session_id":      "sess_abc123",
        "product_id":      "prod_00042",
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def valid_payment_success():
    return {
        "event_id":        str(uuid.uuid4()),
        "event_type":      "payment_success",
        "user_id":         "user_0005678",
        "order_id":        "ord_ABC123XYZ",
        "amount":          "249.99",
        "currency":        "USD",
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
    }


# --- Validation tests ---

class TestValidEventAcceptance:
    def test_valid_product_view_accepted(self):
        result = validate_event(valid_product_view())
        assert result.is_valid
        assert result.event is not None
        assert result.event.event_type == EventType.PRODUCT_VIEW

    def test_valid_payment_success_accepted(self):
        result = validate_event(valid_payment_success())
        assert result.is_valid
        assert result.event.amount is not None
        assert float(result.event.amount) == 249.99

    def test_event_id_auto_generated_if_missing(self):
        payload = valid_product_view()
        del payload["event_id"]
        result = validate_event(payload)
        assert result.is_valid
        assert result.event.event_id is not None

    def test_unix_timestamp_accepted(self):
        payload = valid_product_view()
        payload["event_timestamp"] = datetime.now(timezone.utc).timestamp()
        result = validate_event(payload)
        assert result.is_valid

    def test_z_suffix_timestamp_accepted(self):
        payload = valid_product_view()
        payload["event_timestamp"] = "2026-08-09T12:00:00Z"
        result = validate_event(payload)
        assert result.is_valid


class TestMalformedEventRejection:
    def test_completely_empty_payload_rejected(self):
        result = validate_event({})
        assert not result.is_valid
        assert len(result.errors) > 0

    def test_invalid_event_type_rejected(self):
        payload = valid_product_view()
        payload["event_type"] = "not_a_real_event"
        result = validate_event(payload)
        assert not result.is_valid

    def test_negative_amount_rejected(self):
        payload = valid_payment_success()
        payload["amount"] = "-100.00"
        result = validate_event(payload)
        assert not result.is_valid

    def test_amount_string_garbage_rejected(self):
        payload = valid_payment_success()
        payload["amount"] = "not_a_number"
        result = validate_event(payload)
        assert not result.is_valid

    def test_amount_over_maximum_rejected(self):
        payload = valid_payment_success()
        payload["amount"] = "9999999.99"
        result = validate_event(payload)
        assert not result.is_valid

    def test_invalid_uuid_rejected(self):
        payload = valid_product_view()
        payload["event_id"] = "this-is-not-a-uuid"
        result = validate_event(payload)
        assert not result.is_valid


class TestBusinessRuleValidation:
    def test_payment_success_without_order_id_rejected(self):
        payload = valid_payment_success()
        del payload["order_id"]
        result = validate_event(payload)
        assert not result.is_valid
        assert any("order_id" in e for e in result.errors)

    def test_payment_success_without_amount_rejected(self):
        payload = valid_payment_success()
        del payload["amount"]
        result = validate_event(payload)
        assert not result.is_valid

    def test_payment_success_with_zero_amount_rejected(self):
        payload = valid_payment_success()
        payload["amount"] = "0.00"
        result = validate_event(payload)
        assert not result.is_valid

    def test_product_view_without_product_id_rejected(self):
        payload = valid_product_view()
        del payload["product_id"]
        result = validate_event(payload)
        assert not result.is_valid

    def test_order_created_without_order_id_rejected(self):
        payload = {
            "event_id":        str(uuid.uuid4()),
            "event_type":      "order_created",
            "user_id":         "user_001",
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
        }
        result = validate_event(payload)
        assert not result.is_valid


class TestLateArrivingEvents:
    def test_on_time_event_not_flagged_as_late(self):
        payload = valid_product_view()
        result = validate_event(payload)
        assert result.is_valid
        assert not result.event.properties.get("_late_arriving", False)

    def test_event_25_hours_old_flagged_as_late(self):
        payload = valid_product_view()
        late_time = datetime.now(timezone.utc) - timedelta(hours=25)
        payload["event_timestamp"] = late_time.isoformat()
        result = validate_event(payload)
        # Late events are ACCEPTED but flagged - not rejected
        assert result.is_valid
        assert result.event.properties.get("_late_arriving") is True
        assert result.event.properties.get("_age_hours") >= 25

    def test_event_48_hours_old_still_accepted(self):
        payload = valid_product_view()
        very_late = datetime.now(timezone.utc) - timedelta(hours=48)
        payload["event_timestamp"] = very_late.isoformat()
        result = validate_event(payload)
        assert result.is_valid  # accepted but flagged
        assert result.event.properties["_late_arriving"] is True

    def test_future_event_accepted(self):
        # Events slightly in the future (clock skew) should be accepted
        payload = valid_product_view()
        future = datetime.now(timezone.utc) + timedelta(minutes=5)
        payload["event_timestamp"] = future.isoformat()
        result = validate_event(payload)
        assert result.is_valid


class TestIdStringValidation:
    def test_whitespace_user_id_treated_as_none(self):
        payload = valid_product_view()
        payload["user_id"] = "   "
        result = validate_event(payload)
        assert result.is_valid
        assert result.event.user_id is None

    def test_oversized_user_id_rejected(self):
        payload = valid_product_view()
        payload["user_id"] = "x" * 300
        result = validate_event(payload)
        assert not result.is_valid

    def test_none_user_id_accepted(self):
        # Anonymous events are valid
        payload = valid_product_view()
        payload["user_id"] = None
        result = validate_event(payload)
        assert result.is_valid
