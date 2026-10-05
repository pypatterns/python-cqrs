"""Corner-case coverage for recursive dataclass → dict conversion.

Guards functional parity with ``dataclasses.asdict`` (structure conversion)
while ``DCRequest`` / ``DCResponse.to_dict`` use the faster ``to_plain_dict``
helper that skips leaf ``deepcopy``.
"""

from __future__ import annotations

import dataclasses
import datetime
import enum
import typing
import uuid
from collections import namedtuple
from collections.abc import Mapping

import pytest

from cqrs._dataclass_utils import to_plain_dict
from cqrs.models.request import DCRequest
from cqrs.models.response import DCResponse


# ---------------------------------------------------------------------------
# Shared fixtures / models
# ---------------------------------------------------------------------------


class Color(enum.Enum):
    RED = "red"
    BLUE = "blue"


@dataclasses.dataclass
class Address:
    city: str
    zip_code: str


@dataclasses.dataclass
class Profile:
    name: str
    address: Address
    tags: list[str]


@dataclasses.dataclass(frozen=True)
class FrozenPoint:
    x: int
    y: int


PointNT = namedtuple("PointNT", ["x", "y"])


class NestedRequest(DCRequest):
    user_id: str
    profile: Profile
    meta: dict[str, typing.Any]


class NestedResponse(DCResponse):
    ok: bool
    profile: Profile
    items: list[Address]


class ComplexRequest(DCRequest):
    event_id: uuid.UUID
    created_at: datetime.datetime
    color: Color
    optional: str | None
    addresses: list[Address]
    points: tuple[FrozenPoint, ...]
    named: PointNT
    by_city: dict[str, Address]
    nested_plain: dict[str, list[dict[str, str]]]
    empty_list: list[str]
    empty_dict: dict[str, str]
    flags: tuple[str, ...]


def _sample_profile() -> Profile:
    return Profile(
        name="alice",
        address=Address(city="Berlin", zip_code="10115"),
        tags=["a", "b"],
    )


# ---------------------------------------------------------------------------
# Helper-level tests
# ---------------------------------------------------------------------------


class TestToPlainDictBasics:
    def test_flat_dataclass(self) -> None:
        assert to_plain_dict(Address(city="NYC", zip_code="10001")) == {
            "city": "NYC",
            "zip_code": "10001",
        }

    def test_none_and_scalars_passthrough(self) -> None:
        assert to_plain_dict(None) is None
        assert to_plain_dict(42) == 42
        assert to_plain_dict("x") == "x"
        assert to_plain_dict(True) is True

    def test_empty_containers(self) -> None:
        @dataclasses.dataclass
        class Empty:
            items: list[str]
            mapping: dict[str, int]
            flags: tuple[str, ...]

        result = to_plain_dict(Empty(items=[], mapping={}, flags=()))
        assert result == {"items": [], "mapping": {}, "flags": ()}
        assert isinstance(result["flags"], tuple)

    def test_nested_dataclass(self) -> None:
        profile = _sample_profile()
        result = to_plain_dict(profile)
        assert result == {
            "name": "alice",
            "address": {"city": "Berlin", "zip_code": "10115"},
            "tags": ["a", "b"],
        }
        assert isinstance(result["address"], dict)
        assert not dataclasses.is_dataclass(result["address"])

    def test_list_of_dataclasses(self) -> None:
        items = [Address(city="A", zip_code="1"), Address(city="B", zip_code="2")]
        assert to_plain_dict(items) == [
            {"city": "A", "zip_code": "1"},
            {"city": "B", "zip_code": "2"},
        ]

    def test_tuple_of_dataclasses_preserves_tuple(self) -> None:
        items = (FrozenPoint(1, 2), FrozenPoint(3, 4))
        result = to_plain_dict(items)
        assert result == ({"x": 1, "y": 2}, {"x": 3, "y": 4})
        assert isinstance(result, tuple)

    def test_namedtuple_preserved_and_nested_converted(self) -> None:
        @dataclasses.dataclass
        class Wrap:
            point: PointNT
            nested: tuple[Address, ...]

        result = to_plain_dict(
            Wrap(point=PointNT(10, 20), nested=(Address("X", "1"),)),
        )
        assert result["point"] == PointNT(10, 20)
        assert type(result["point"]) is PointNT
        assert result["nested"] == ({"city": "X", "zip_code": "1"},)

    def test_dict_of_dataclasses(self) -> None:
        mapping = {
            "home": Address(city="Berlin", zip_code="10115"),
            "work": Address(city="Munich", zip_code="80331"),
        }
        assert to_plain_dict(mapping) == {
            "home": {"city": "Berlin", "zip_code": "10115"},
            "work": {"city": "Munich", "zip_code": "80331"},
        }

    def test_dataclass_dict_keys_are_preserved(self) -> None:
        """Dataclass keys stay hashable (asdict would crash converting them)."""
        key = FrozenPoint(1, 2)
        mapping = {key: Address(city="A", zip_code="1")}
        result = to_plain_dict(mapping)
        assert result == {key: {"city": "A", "zip_code": "1"}}
        assert key in result

    def test_uuid_datetime_enum_preserved_as_objects(self) -> None:
        uid = uuid.uuid4()
        ts = datetime.datetime(2026, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)

        @dataclasses.dataclass
        class Row:
            event_id: uuid.UUID
            created_at: datetime.datetime
            color: Color
            optional: str | None

        result = to_plain_dict(Row(event_id=uid, created_at=ts, color=Color.RED, optional=None))
        assert result["event_id"] is uid
        assert result["created_at"] is ts
        assert result["color"] is Color.RED
        assert result["optional"] is None

    def test_deep_mixed_nesting(self) -> None:
        @dataclasses.dataclass
        class Deep:
            tree: dict[str, list[dict[str, Address | Profile | str]]]

        obj = Deep(
            tree={
                "branch": [
                    {
                        "addr": Address("C", "1"),
                        "profile": _sample_profile(),
                        "label": "ok",
                    },
                ],
            },
        )
        result = to_plain_dict(obj)
        assert result["tree"]["branch"][0]["addr"] == {"city": "C", "zip_code": "1"}
        assert result["tree"]["branch"][0]["profile"]["address"]["city"] == "Berlin"
        assert result["tree"]["branch"][0]["label"] == "ok"

    def test_container_isolation_from_source(self) -> None:
        profile = _sample_profile()
        result = to_plain_dict(profile)
        result["tags"].append("mutated")
        result["address"]["city"] = "mutated"
        assert profile.tags == ["a", "b"]
        assert profile.address.city == "Berlin"

    def test_custom_mapping_subclass_preserved(self) -> None:
        class StrDict(dict):
            pass

        @dataclasses.dataclass
        class Wrap:
            data: StrDict

        raw = StrDict(a=1)
        result = to_plain_dict(Wrap(data=raw))
        assert isinstance(result["data"], StrDict)
        assert result["data"] == {"a": 1}
        assert result["data"] is not raw


def _asdict_compatible(obj: typing.Any) -> typing.Any:
    """Apply ``dataclasses.asdict`` rules to arbitrary roots (list/dict/…)."""
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return dataclasses.asdict(typing.cast(typing.Any, obj))
    if isinstance(obj, tuple) and hasattr(obj, "_fields"):
        return type(obj)(*(_asdict_compatible(v) for v in obj))
    if isinstance(obj, (list, tuple)):
        return type(obj)(_asdict_compatible(v) for v in obj)
    if isinstance(obj, dict):
        return type(obj)((_asdict_compatible(k), _asdict_compatible(v)) for k, v in obj.items())
    return obj


class TestToPlainDictMatchesAsdict:
    """Structural parity with ``dataclasses.asdict`` on representative trees."""

    @pytest.mark.parametrize(
        "obj",
        [
            Address(city="A", zip_code="1"),
            _sample_profile(),
            [Address("A", "1"), Address("B", "2")],
            (FrozenPoint(1, 2), FrozenPoint(3, 4)),
            {"k": Address("A", "1")},
            PointNT(1, 2),
            {
                "level1": {
                    "group": [
                        {"addr": Address("X", "1"), "n": 1},
                        {"addr": Address("Y", "2"), "n": 2},
                    ],
                },
                "level2": [FrozenPoint(9, 9)],
            },
        ],
    )
    def test_equals_asdict_tree(self, obj: typing.Any) -> None:
        assert to_plain_dict(obj) == _asdict_compatible(obj)


# ---------------------------------------------------------------------------
# DCRequest / DCResponse.to_dict integration
# ---------------------------------------------------------------------------


class TestDCRequestResponseToDict:
    def test_nested_request_to_dict(self) -> None:
        req = NestedRequest(
            user_id="u1",
            profile=_sample_profile(),
            meta={"role": "admin", "score": 10},
        )
        data = req.to_dict()
        assert data["user_id"] == "u1"
        assert data["profile"]["address"] == {"city": "Berlin", "zip_code": "10115"}
        assert data["meta"] == {"role": "admin", "score": 10}
        assert isinstance(data["profile"], dict)

    def test_nested_response_to_dict(self) -> None:
        resp = NestedResponse(
            ok=True,
            profile=_sample_profile(),
            items=[Address("A", "1"), Address("B", "2")],
        )
        data = resp.to_dict()
        assert data["ok"] is True
        assert data["items"] == [
            {"city": "A", "zip_code": "1"},
            {"city": "B", "zip_code": "2"},
        ]

    def test_complex_request_corner_cases(self) -> None:
        uid = uuid.uuid4()
        ts = datetime.datetime(2026, 5, 6, 7, 8, 9, tzinfo=datetime.timezone.utc)
        req = ComplexRequest(
            event_id=uid,
            created_at=ts,
            color=Color.BLUE,
            optional=None,
            addresses=[Address("Berlin", "10115")],
            points=(FrozenPoint(1, 2), FrozenPoint(3, 4)),
            named=PointNT(5, 6),
            by_city={"berlin": Address("Berlin", "10115")},
            nested_plain={
                "group1": [{"name": "item1", "value": "val1"}] * 2,
                "group2": [{"name": "item2", "value": "val2"}],
            },
            empty_list=[],
            empty_dict={},
            flags=("x", "y"),
        )
        data = req.to_dict()

        assert data["event_id"] is uid
        assert data["created_at"] is ts
        assert data["color"] is Color.BLUE
        assert data["optional"] is None
        assert data["addresses"] == [{"city": "Berlin", "zip_code": "10115"}]
        assert data["points"] == ({"x": 1, "y": 2}, {"x": 3, "y": 4})
        assert data["named"] == PointNT(5, 6)
        assert type(data["named"]) is PointNT
        assert data["by_city"] == {"berlin": {"city": "Berlin", "zip_code": "10115"}}
        assert data["nested_plain"]["group1"][0] == {"name": "item1", "value": "val1"}
        assert data["empty_list"] == []
        assert data["empty_dict"] == {}
        assert data["flags"] == ("x", "y")

        # Mutating the serialized tree must not affect the source request.
        data["addresses"].append({"city": "X", "zip_code": "0"})
        data["nested_plain"]["group1"].append({"name": "z", "value": "z"})
        assert len(req.addresses) == 1
        assert len(req.nested_plain["group1"]) == 2

    def test_matches_dataclasses_asdict_for_nested_request(self) -> None:
        req = NestedRequest(
            user_id="u1",
            profile=_sample_profile(),
            meta={"k": [1, 2, {"z": 3}]},
        )
        assert req.to_dict() == dataclasses.asdict(req)

    def test_matches_dataclasses_asdict_for_complex_request(self) -> None:
        req = ComplexRequest(
            event_id=uuid.uuid4(),
            created_at=datetime.datetime.now(datetime.timezone.utc),
            color=Color.RED,
            optional="x",
            addresses=[Address("A", "1")],
            points=(FrozenPoint(1, 2),),
            named=PointNT(3, 4),
            by_city={"a": Address("A", "1")},
            nested_plain={"g": [{"n": "1"}]},
            empty_list=[],
            empty_dict={},
            flags=(),
        )
        assert req.to_dict() == dataclasses.asdict(req)

    def test_from_dict_round_trip_flat_fields(self) -> None:
        class Flat(DCRequest):
            user_id: str
            count: int

        original = Flat(user_id="u1", count=3)
        restored = Flat.from_dict(**original.to_dict())
        assert restored == original

    def test_mapping_abc_not_treated_as_dict_subclass_only(self) -> None:
        """Plain Mapping that is not a dict stays a leaf (same as asdict deepcopy leaf)."""

        class Readonly(Mapping):
            def __init__(self, data: dict) -> None:
                self._data = data

            def __getitem__(self, key):
                return self._data[key]

            def __iter__(self):
                return iter(self._data)

            def __len__(self):
                return len(self._data)

        payload = Readonly({"a": 1})

        @dataclasses.dataclass
        class Wrap:
            data: Readonly

        result = to_plain_dict(Wrap(data=payload))
        # Not a dict subclass → leaf, shared reference (no deepcopy).
        assert result["data"] is payload
