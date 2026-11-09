import typing

T = typing.TypeVar("T")

class Provider(typing.Generic[T]):
    cls: typing.Any

    def __call__(self, *args: typing.Any, **kwargs: typing.Any) -> typing.Any: ...

class Container(Provider[typing.Any]):
    providers: dict[str, Provider[typing.Any]]
