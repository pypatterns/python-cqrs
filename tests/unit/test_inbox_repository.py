import cqrs
from cqrs.inbox.memory import MemoryInboxRepository


class Deposit(cqrs.Request):
    account_id: str
    amount: str


class OrderCreated(cqrs.DomainEvent):
    order_id: str


def _map() -> cqrs.InboxMessageMap:
    message_map = cqrs.InboxMessageMap()
    message_map.register("Deposit", Deposit)
    message_map.register("OrderCreated", OrderCreated)
    return message_map


def _deposit(account_id: str = "acc-1", amount: str = "10.00") -> Deposit:
    return Deposit(account_id=account_id, amount=amount)


async def test_empty_batch_returns_empty_list():
    repository = MemoryInboxRepository(_map())

    assert await repository.add([]) == []


async def test_single_and_several_items_return_one_bool_per_item():
    repository = MemoryInboxRepository(_map())

    assert await repository.add([("only", _deposit())]) == [True]
    stored = await repository.add(
        [
            ("m1", _deposit("acc-1")),
            ("m2", _deposit("acc-2", "5.00")),
        ],
    )

    assert stored == [True, True]
    assert len(await repository.get_many()) == 3


async def test_duplicate_inside_batch_and_already_stored_key_are_false():
    repository = MemoryInboxRepository(_map())
    first = await repository.add(
        [
            ("m1", _deposit("acc-1")),
            ("m1", _deposit("acc-1")),
            ("m2", _deposit("acc-2", "5.00")),
        ],
    )
    assert first == [True, False, True]

    second = await repository.add(
        [
            ("m1", _deposit("acc-1")),
            ("m3", _deposit("acc-3", "1.00")),
        ],
    )
    assert second == [False, True]

    same_id_other_name = await repository.add(
        [
            ("m1", OrderCreated(order_id="ord-1")),
        ],
    )
    assert same_id_other_name == [True]


async def test_get_many_respects_batch_size_and_skips_processed():
    repository = MemoryInboxRepository(_map())
    await repository.add(
        [
            ("m1", _deposit("acc-1")),
            ("m2", _deposit("acc-2")),
            ("m3", _deposit("acc-3")),
        ],
    )

    first = await repository.get_many(batch_size=2)
    assert [row.message_id for row in first] == ["m1", "m2"]
    assert [row.id for row in first] == sorted(row.id for row in first)

    await repository.update_status(first[0].id, cqrs.InboxStatus.PROCESSED)
    rest = await repository.get_many(batch_size=10)
    assert [row.message_id for row in rest] == ["m2", "m3"]


async def test_row_leaves_get_many_after_five_not_processed():
    repository = MemoryInboxRepository(_map())
    await repository.add([("m1", _deposit())])
    [row] = await repository.get_many()

    for attempt in range(4):
        await repository.update_status(row.id, cqrs.InboxStatus.NOT_PROCESSED)
        still_there = await repository.get_many()
        assert len(still_there) == 1
        assert still_there[0].flush_counter == attempt + 1

    await repository.update_status(row.id, cqrs.InboxStatus.NOT_PROCESSED)

    assert await repository.get_many() == []
    assert repository._rows[0].flush_counter == 5
    assert repository._rows[0].status is cqrs.InboxStatus.NOT_PROCESSED
