"""
Event schema definitions for PulseFlow.

Pydantic models serve as the contract between producers and consumers.
Any event that doesn't match these schemas goes to the dead letter queue.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class EventType(str, Enum):
    PRODUCT_VIEW     = "product_view"
    SEARCH           = "search"
    ADD_TO_CART      = "add_to_cart"
    REMOVE_FROM_CART = "remove_from_cart"
    CHECKOUT         = "checkout"
    PAYMENT_SUCCESS  = "payment_success"
    PAYMENT_FAILURE  = "payment_failure"
    ORDER_CREATED    = "order_created"
    ORDER_CANCELLED  = "order_cancelled"
    USER_LOGIN       = "user_login"
    USER_LOGOUT      = "user_logout"
    PAGE_VIEW        = "page_view"


class Currency(str, Enum):
    USD = "USD"
    EUR = "EUR"
    GBP = "GBP"
    CAD = "CAD"


class EcommerceEvent(BaseModel):
    """
    The canonical event schema. Every event flowing through PulseFlow
    must conform to this structure.

    Required fields are intentionally minimal - we want to accept events
    even if they're missing optional context, and handle missing data
    gracefully rather than rejecting everything.
    """

    event_id:        uuid.UUID  = Field(default_factory=uuid.uuid4)
    event_type:      EventType
    user_id:         Optional[str] = None
    session_id:      Optional[str] = None
    product_id:      Optional[str] = None
    order_id:        Optional[str] = None
    amount:          Optional[Decimal] = None
    currency:        Currency = Currency.USD
    properties:      dict[str, Any] = Field(default_factory=dict)
    client_ip:       Optional[str] = None
    user_agent:      Optional[str] = None
    # Producers set this - allows us to detect late-arriving events
    event_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = {"use_enum_values": True}

    @field_validator("event_id", mode="before")
    @classmethod
    def parse_uuid(cls, v: Any) -> uuid.UUID:
        if isinstance(v, uuid.UUID):
            return v
        try:
            return uuid.UUID(str(v))
        except (ValueError, AttributeError) as exc:
            raise ValueError(f"Invalid UUID format: {v}") from exc

    @field_validator("amount", mode="before")
    @classmethod
    def validate_amount(cls, v: Any) -> Optional[Decimal]:
        if v is None:
            return None
        try:
            amount = Decimal(str(v))
        except Exception as exc:
            raise ValueError(f"Amount must be numeric, got: {v}") from exc
        if amount < 0:
            raise ValueError(f"Amount cannot be negative: {amount}")
        if amount > Decimal("1000000"):
            raise ValueError(f"Amount exceeds maximum allowed value: {amount}")
        return amount

    @field_validator("event_timestamp", mode="before")
    @classmethod
    def ensure_timezone(cls, v: Any) -> datetime:
        if isinstance(v, str):
            dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
        elif isinstance(v, (int, float)):
            # Accept Unix timestamps
            dt = datetime.fromtimestamp(v, tz=timezone.utc)
        elif isinstance(v, datetime):
            dt = v
        else:
            raise ValueError(f"Cannot parse timestamp: {v}")

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    @field_validator("user_id", "session_id", "product_id", "order_id", mode="before")
    @classmethod
    def sanitize_string_ids(cls, v: Any) -> Optional[str]:
        if v is None:
            return None
        s = str(v).strip()
        if len(s) > 255:
            raise ValueError(f"ID field exceeds max length (255): {len(s)} chars")
        if not s:
            return None
        return s

    @model_validator(mode="after")
    def validate_business_rules(self) -> EcommerceEvent:
        """
        Cross-field validation. Business logic that spans multiple fields.
        These are the validations that make your pipeline actually reliable.
        """
        # Payment and order events must have an order_id
        financial_events = {
            EventType.PAYMENT_SUCCESS,
            EventType.PAYMENT_FAILURE,
            EventType.ORDER_CREATED,
            EventType.ORDER_CANCELLED,
        }
        if self.event_type in financial_events and not self.order_id:
            raise ValueError(
                f"order_id is required for event_type '{self.event_type}'"
            )

        # Payment success must have a positive amount
        if self.event_type == EventType.PAYMENT_SUCCESS:
            if self.amount is None or self.amount <= 0:
                raise ValueError(
                    "payment_success event requires a positive amount"
                )

        # Product interactions need a product_id
        product_events = {EventType.PRODUCT_VIEW, EventType.ADD_TO_CART, EventType.REMOVE_FROM_CART}
        if self.event_type in product_events and not self.product_id:
            raise ValueError(
                f"product_id is required for event_type '{self.event_type}'"
            )

        # Late-arriving event detection (more than 24 hours old)
        now = datetime.now(timezone.utc)
        age_hours = (now - self.event_timestamp).total_seconds() / 3600
        if age_hours > 24:
            # We don't reject late events - we just flag them in properties
            # so downstream systems can handle them appropriately
            self.properties["_late_arriving"] = True
            self.properties["_age_hours"] = round(age_hours, 2)

        return self


class ValidationResult(BaseModel):
    """Result of validating a raw event payload."""
    is_valid:          bool
    event:             Optional[EcommerceEvent] = None
    errors:            list[str] = Field(default_factory=list)
    raw_payload:       dict[str, Any] = Field(default_factory=dict)
    validation_time_ms: float = 0.0


class ProcessingResult(BaseModel):
    """Result of processing a single event through the full pipeline."""
    event_id:       Optional[str] = None
    success:        bool
    is_duplicate:   bool = False
    is_late:        bool = False
    error_message:  Optional[str] = None
    processing_time_ms: float = 0.0
