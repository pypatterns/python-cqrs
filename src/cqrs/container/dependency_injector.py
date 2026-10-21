from typing import TypeVar, Optional, cast
import inspect
import functools
from dependency_injector import providers
from dependency_injector.containers import Container as DependencyInjectorContainer
from cqrs.container.protocol import Container as CQRSContainerProtocol

T = TypeVar("T")


class DependencyInjectorCQRSContainer(
    CQRSContainerProtocol[DependencyInjectorContainer],
):
    """
    Adapter bridging dependency-injector containers with CQRS framework.

    This adapter enables seamless integration between the dependency-injector library
    and the CQRS framework by providing type-based dependency resolution for request
    and event handlers. It automatically discovers all providers in the container
    hierarchy and builds an internal mapping that enables efficient resolution.

    Key Features:
        - Automatic provider discovery through recursive container traversal
        - Type-based resolution without manual registration
        - Support for nested container hierarchies
        - Two-strategy resolution: exact type match and inheritance-based matching
        - Performance optimization through caching
        - Clean state management when re-attaching containers

    Resolution Strategies:
        1. Exact Match: Directly resolves registered concrete types
        2. Inheritance Match: Resolves abstract interfaces by finding their concrete implementations
           (This enables Dependency Inversion Principle - depend on abstractions, not concretions)

    Usage Pattern:
        >>> from dependency_injector import containers, providers
        >>>
        >>> # Define your dependency-injector container
        >>> class Container(containers.DeclarativeContainer):
        ...     database = providers.Singleton(Database)
        ...     user_repository = providers.Factory(UserRepository, db=database)
        ...     user_service = providers.Factory(UserService, repo=user_repository)
        >>>
        >>> # Attach to CQRS framework
        >>> cqrs_container = DependencyInjectorCQRSContainer()
        >>> cqrs_container.attach_external_container(Container())
        >>>
        >>> # Resolve dependencies by type in your handlers
        >>> service = await cqrs_container.resolve(UserService)
        >>> # Or resolve by abstract interface
        >>> service = await cqrs_container.resolve(IUserService)

    [AI Instruction]
    Wrap a dependency-injector declarative container and pass this object to
    ``bootstrap``. Call ``attach_external_container`` before the first
    resolve. Providers are discovered by type. This is not ``DIContainer``,
    which wraps the ``di`` package.
    """

    def __init__(self) -> None:
        """
        Initialize an empty CQRS container adapter.

        The container starts in an unattached state. You must call
        attach_external_container() before attempting to resolve any dependencies.
        """
        # Reference to the wrapped dependency-injector container
        self._container: Optional[DependencyInjectorContainer] = None

        # Internal mapping from types to their provider access paths
        # Example: UserService → ("user_service",)
        # Example: EmailService → ("subdomain", "email_service")
        # This path-based approach enables late binding and supports provider overrides
        self._type_to_provider_path_map: dict[type, tuple[str, ...]] = {}

    @property
    def external_container(self) -> DependencyInjectorContainer:
        """
        Get the attached external dependency-injector container.

        This property provides direct access to the underlying dependency-injector
        container, which can be useful for advanced scenarios like manual provider
        inspection or runtime container manipulation.

        Returns:
            The wrapped dependency-injector container instance.

        Raises:
            ValueError: If no external container has been attached yet via
                attach_external_container().

        Note:
            In most cases, you should use resolve() instead of accessing the
            external container directly.
        """
        if self._container is None:
            raise ValueError("External container not attached")
        return self._container

    def attach_external_container(self, container: DependencyInjectorContainer) -> None:
        """
        Attach an external dependency-injector container and build type-to-provider mappings.

        This method performs three critical operations:
        1. Stores a reference to the external container
        2. Recursively traverses the entire container hierarchy to discover all providers
        3. Builds an internal map from types to their provider access paths

        The traversal process examines all providers in the container, including those
        in nested sub-containers, and records the dotted path to reach each provider.
        This enables efficient type-based lookup during resolution.

        Args:
            container: A configured dependency-injector container instance (typically
                a DeclarativeContainer subclass with providers defined as class attributes).

        Behavior on Multiple Calls:
            If called multiple times, this method completely resets the internal state:
            - Clears the previous type-to-provider mapping
            - Clears the provider lookup cache
            - Rebuilds mappings from the new container
            This allows hot-swapping containers but invalidates any previous resolutions.

        Example:
            >>> class Container(containers.DeclarativeContainer):
            ...     user_service = providers.Factory(UserService)
            ...     email_service = providers.Singleton(EmailService)
            ...
            ...     # Nested container
            ...     notifications = providers.Container(NotificationContainer)
            >>>
            >>> cqrs_container = DependencyInjectorCQRSContainer()
            >>> cqrs_container.attach_external_container(Container())
            >>> # Now ready to resolve UserService, EmailService, and nested providers

        Note:
            This should typically be called once during application initialization,
            before any handlers attempt to resolve dependencies.
        """
        self._container = container
        self._type_to_provider_path_map.clear()
        self._get_provider.cache_clear()
        self._traverse_container(container)

    async def resolve(self, type_: type[T]) -> T:
        """
        Resolve and instantiate a dependency by its type.

        This is the primary method for obtaining dependencies in CQRS handlers.
        It uses a two-strategy approach to find the appropriate provider and
        then invokes it to create or retrieve an instance.

        Resolution Strategies (executed in order):
            1. Exact Type Match:
               Searches for a provider explicitly registered for the requested type.
               Example: resolve(UserService) finds providers.Factory(UserService, ...)

            2. Inheritance-Based Match:
               If no exact match exists, searches for a provider registered for a
               subclass of the requested type. This enables resolving abstract base
               classes or interfaces to their concrete implementations.
               Example: resolve(IUserService) finds providers.Factory(UserService, ...)
                        where UserService implements IUserService

        Args:
            type_: The type/class to resolve. Can be either a concrete class or an
                abstract base class/interface.

        Returns:
            An instance of the requested type. The instance lifecycle (singleton,
            factory, etc.) is determined by the provider configuration in the
            external container.

        Raises:
            ValueError: If no provider can be found for the requested type using
                either resolution strategy.

        Performance:
            Provider lookup is cached using functools.cache. The first resolution
            of a type performs a full lookup; subsequent resolutions use the cached
            provider. The provider itself is invoked on every call, respecting its
            configured scope (Factory creates new instances, Singleton reuses one).

        Example:
            >>> # Resolve concrete type
            >>> repository = await container.resolve(UserRepository)
            >>>
            >>> # Resolve abstract interface (uses inheritance matching)
            >>> service = await container.resolve(IUserService)  # Returns UserService instance
            >>>
            >>> # In a CQRS handler
            >>> class CreateUserHandler:
            ...     async def handle(self, request, container):
            ...         service = await container.resolve(UserService)
            ...         return await service.create_user(request.name)
        """
        provider = self._get_provider(type_)
        result = provider()
        # If provider returns a coroutine or Future (async provider), await it
        # Note: inspect.iscoroutine() only checks for coroutines, not Futures/Tasks
        # We need to check for any awaitable object
        if inspect.isawaitable(result):
            return await result
        return result

    def _traverse_container(
        self,
        container: DependencyInjectorContainer,
        parent_path_segments: Optional[list[str]] = None,
    ) -> None:
        """
        Recursively traverse container hierarchy to discover and map all providers.

        This method performs a depth-first traversal of the container structure,
        visiting every provider and building a map from types to their access paths.
        It handles both simple providers and nested container hierarchies.

        Args:
            container: The container to traverse (can be root or nested container).
            parent_path_segments: Accumulator tracking the path from root to current
                container level. For nested containers, this builds up the full path
                (e.g., ["subdomain", "notifications"]).

        Traversal Process:
            1. Iterates through all providers in the container
            2. Builds the current path by appending provider name to parent path
            3. For Container providers: recursively traverses the nested container
            4. For class-based providers (Factory, Singleton, etc.): records the
               mapping from type to path

        Provider Types Mapped:
            - Factory, Singleton, Callable, Coroutine providers with a 'cls' attribute
            - Only providers that reference actual classes (verified via isclass())

        Provider Types Ignored:
            - Container providers (traversed but not mapped themselves)
            - Configuration, Resource, and other non-class providers
            - Providers without a 'cls' attribute

        Example Mappings Created:
            UserService → ("user_service",)
            EmailService → ("notifications", "email_service")
            Database → ("infrastructure", "database", "connection")

        Note:
            This method mutates self._type_to_provider_path_map by adding entries
            during traversal. It does not return a value.
        """
        if parent_path_segments is None:
            parent_path_segments = []

        for provider_name, provider in container.providers.items():
            # Build the complete path to this provider
            current_path = [*parent_path_segments, provider_name]

            # Handle nested containers recursively
            if isinstance(provider, providers.Container):
                nested_container = cast(DependencyInjectorContainer, provider)
                self._traverse_container(nested_container, current_path)

            # Map class-based providers to their types
            # Only providers with a 'cls' attribute pointing to actual classes are mapped
            if not hasattr(provider, "cls"):
                continue

            # Get the class from the provider
            c = getattr(provider, "cls")

            # Map class-based providers to their types
            if inspect.isclass(c):
                self._type_to_provider_path_map[c] = tuple(current_path)

            # Map function providers to their return types
            # Only providers with a return type that is a class are mapped
            elif inspect.isfunction(c):
                return_type = inspect.signature(c).return_annotation
                if inspect.isclass(return_type):
                    self._type_to_provider_path_map[return_type] = tuple(current_path)

    def _get_provider_by_path_segments(
        self,
        path_segments: tuple[str, ...],
    ) -> providers.Provider[object]:
        """
        Navigate container hierarchy to retrieve a provider by its access path.

        This method takes a path like ("database", "session") and resolves it to
        the actual provider object by performing attribute access on the container:
        container.database.session

        Args:
            path_segments: Tuple of attribute names representing the dotted path
                from the root container to the target provider.
                Examples:
                - ("user_service",) → container.user_service
                - ("subdomain", "email_service") → container.subdomain.email_service
                - ("infra", "db", "pool") → container.infra.db.pool

        Returns:
            The provider object located at the specified path. This is typically
            a Factory, Singleton, or other provider instance.

        Raises:
            AttributeError: If any segment in the path doesn't exist on the container
                or intermediate object.

        Implementation:
            Uses functools.reduce to iteratively perform getattr() on each segment,
            starting from the root container and walking down the hierarchy.

        Note:
            This method is typically called internally by _get_provider() and should
            not need to be called directly by users of this class.
        """
        return functools.reduce(
            lambda obj, segment: getattr(obj, segment),
            path_segments,
            self._container,
        )

    @functools.cache
    def _get_provider(
        self,
        requested_type: type[T],
    ) -> providers.Provider[T]:
        """
        Find and return the provider for a requested type with caching.

        This method implements the two-strategy resolution algorithm that enables
        both direct type lookup and inheritance-based resolution. Results are
        cached permanently using functools.cache for maximum performance.

        Resolution Algorithm:
            Strategy 1 - Exact Type Match (O(1) lookup):
                Checks if the requested type exists directly in the type-to-provider map.
                This handles cases where the exact type was registered in the container.

            Strategy 2 - Inheritance-Based Match (O(n) scan):
                If no exact match, iterates through all registered types to find one
                that is a subclass of the requested type. This enables:
                - Resolving abstract base classes to concrete implementations
                - Resolving interfaces to their implementations
                - Programming to abstractions instead of concretions

        Args:
            requested_type: The type to find a provider for. Can be concrete or abstract.

        Returns:
            The provider that can instantiate the requested type. The provider will
            be invoked by the caller (typically resolve()) to create instances.

        Raises:
            ValueError: If neither resolution strategy finds a suitable provider.
                This typically means the type was not registered in the container.

        Caching Behavior:
            This method is decorated with @functools.cache, which means:
            - First call for a type performs the full lookup algorithm
            - Subsequent calls for the same type return the cached provider instantly
            - Cache persists for the lifetime of the container instance
            - Cache is cleared when attach_external_container() is called

        Performance Characteristics:
            - Exact match: O(1) dictionary lookup
            - Inheritance match: O(n) where n = number of registered types
            - Cached retrieval: O(1) after first lookup
            - Memory: O(k) where k = number of unique types resolved

        Examples:
            >>> # Exact match
            >>> provider = container._get_provider(UserService)  # Fast O(1)
            >>>
            >>> # Inheritance match (first time)
            >>> provider = container._get_provider(IUserService)  # Slower O(n)
            >>> # Same call (cached)
            >>> provider = container._get_provider(IUserService)  # Fast O(1)

        Note:
            When inheritance matching finds multiple subclasses, it returns the
            first one encountered in the dictionary iteration. For predictable
            behavior, avoid registering multiple implementations of the same interface.
        """
        # Strategy 1: Exact type match
        if requested_type in self._type_to_provider_path_map:
            return cast(
                providers.Provider[T],
                self._get_provider_by_path_segments(
                    self._type_to_provider_path_map[requested_type],
                ),
            )

        # Strategy 2: Inheritance-based match
        # Find any registered type that is a subclass of the requested type
        # This enables resolving abstract base classes to their concrete implementations
        for registered_type in self._type_to_provider_path_map:
            if issubclass(registered_type, requested_type):
                return cast(
                    providers.Provider[T],
                    self._get_provider_by_path_segments(
                        self._type_to_provider_path_map[registered_type],
                    ),
                )

        # No provider found for the requested type
        raise ValueError(
            f"Provider for type {requested_type.__name__} not found. "
            f"Ensure the type is registered in the dependency-injector container.",
        )
