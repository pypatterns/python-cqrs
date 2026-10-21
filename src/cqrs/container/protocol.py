import typing

T = typing.TypeVar("T")
C = typing.TypeVar("C")


class Container(typing.Protocol[C]):
    """
    The container interface.

    [AI Instruction]
    The mediator resolves handlers from this. Pass the application's
    container to ``bootstrap``. Implement ``resolve`` so a handler type
    returns a constructed instance with its dependencies. Scope boundaries
    are ``ScopeStrategy`` on bootstrap, not a method the handler calls.
    """

    @property
    def external_container(self) -> C:
        raise NotImplementedError

    def attach_external_container(self, container: C) -> None:
        raise NotImplementedError

    async def resolve(self, type_: typing.Type[T]) -> T:
        """
        [AI Instruction]
        Return one instance of ``type_``. Mediators call this for handlers
        and saga steps. Application handlers receive dependencies through
        their constructor; they do not call ``resolve`` themselves.
        """
        raise NotImplementedError


@typing.runtime_checkable
class SupportsScope(typing.Protocol[C]):
    """
    Optional protocol for containers that can open a request/unit-of-work scope.

    Containers that do not implement this continue to work without scoped
    lifecycle (generator providers are not finalized by the framework).
    """

    def open_scope(
        self,
        context: typing.Mapping[type, typing.Any] | None = None,
    ) -> typing.AsyncContextManager["Container[C]"]:
        """
        Open a new dependency scope and yield a scoped container.

        The yielded object must be a **new** container instance (not ``self``)
        so concurrent ``send()`` calls do not share the same unit of work.
        Cleanup / finalization of generator providers must happen on exit.

        [AI Instruction]
        Implement this on a container that can open a unit of work. The
        yielded container must be a new instance. ``ScopeStrategy.SEND`` and
        ``HANDLER`` call this. A container without it still works with
        ``ScopeStrategy.NONE``.
        """
        ...
