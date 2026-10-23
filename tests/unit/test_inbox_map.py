import cqrs


class Deposit(cqrs.Request):
    account_id: str
    amount: str


class OrderCreated(cqrs.DomainEvent):
    order_id: str


class Notice(cqrs.NotificationEvent):
    event_name: str = "order_created"
    payload: str


def test_register_request_and_event_round_trip():
    message_map = cqrs.InboxMessageMap()
    message_map.register("Deposit", Deposit)
    message_map.register("OrderCreated", OrderCreated)
    message_map.register("order_created", Notice)

    deposit = Deposit(account_id="acc-1", amount="10.00")
    event = OrderCreated(order_id="ord-1")
    notice = Notice(event_name="order_created", payload="ord-1")

    deposit_name, deposit_payload = message_map.payload_for(deposit)
    event_name, event_payload = message_map.payload_for(event)
    notice_name, notice_payload = message_map.payload_for(notice)

    assert deposit_name == "Deposit"
    assert event_name == "OrderCreated"
    assert notice_name == "order_created"
    assert message_map.deserialize(deposit_name, deposit_payload) == deposit
    assert message_map.deserialize(event_name, event_payload) == event
    restored = message_map.deserialize(notice_name, notice_payload)
    assert restored.event_name == notice.event_name
    assert restored.payload == notice.payload
    assert restored.event_id == notice.event_id


def test_second_register_of_the_same_name_raises_key_error():
    message_map = cqrs.InboxMessageMap()
    message_map.register("Deposit", Deposit)

    try:
        message_map.register("Deposit", OrderCreated)
        raised = False
    except KeyError as exc:
        raised = True
        assert "already registered" in str(exc)

    assert raised


def test_unknown_name_does_not_deserialize():
    message_map = cqrs.InboxMessageMap()

    try:
        message_map.deserialize("missing", b"{}")
        raised = False
    except KeyError as exc:
        raised = True
        assert "Unknown message name" in str(exc)

    assert raised


def test_instance_does_not_share_entries_with_process_singleton():
    message_map = cqrs.InboxMessageMap()
    message_map.register("Deposit", Deposit)

    assert message_map.get("Deposit") is Deposit
    assert cqrs.InboxMessageMap.get("Deposit") is None
    isolated = cqrs.InboxMessageMap()
    assert isolated.get("Deposit") is None
