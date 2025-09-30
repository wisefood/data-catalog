"""
The Entity class is a base class for all catalog entities. The main responsibility
of the Entity class is to provide a common interface for interacting via the
Catalog API. The class defines a set of operations that can be performed
on an entity, such as listing, fetching, creating, updating, and deleting.
The specific implementation of these operations is left to the subclasses.
"""

from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from backend.redis import REDIS
from utils import is_valid_uuid
from main import config
from exceptions import NotAllowedError, DataError, InternalError, NotFoundError, ConflictError
import logging

class Entity:
    """
    Base class for all catalog entities.

    In ReST terminology, an entity is a resource that can be accessed via an API. 

    This class provides the basic structure for all entities. It defines the common 
    operations that can be performed on an entity, such as listing, fetching, creating,
    updating, and deleting. The specific implementation of these operations is left to
    the subclasses.

    The API defined in this class is the one used by the endpoint definitions.
    """

    OPERATIONS = frozenset([
        "list",
        "fetch",
        "get",
        "create",
        "delete",
        "search",
        "patch",
    ])

    def __init__(self, name: str, collection_name: str, creation_schema: BaseModel, update_schema: BaseModel):
        """
        Initialize the entity with its name, collection name, creation schema, and update schema.

        :param name: The name of the entity.
        :param collection_name: The name of the collection of such entities.
        :param creation_schema: The schema used for creating instances of this entity.
        :param update_schema: The schema used for updating instances of this entity.
        """
        self.name = name
        self.collection_name = collection_name
        self.creation_schema = creation_schema
        self.update_schema = update_schema

        self.operations = Entity.OPERATIONS.copy()
        if update_schema is None:
            self.operations.remove("patch")

    def fetch_entities(self, limit: Optional[int] = None, offset: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Fetch a list of entities bundler method.

        :param limit: The maximum number of entities to return.
        :param offset: The number of entities to skip before starting to collect the result set.
        :return: A list of entities.
        """
        return self.fetch(limit=limit, offset=offset)
    
    def fetch(self, limit: Optional[int] = None, offset: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Fetch a list of entities.

        :param limit: The maximum number of entities to return.
        :param offset: The number of entities to skip before starting to collect the result set.
        :return: A list of entities.
        """
        raise NotImplementedError("Subclasses of the Entity class must implement this method.")

    def list_entities(self, limit: Optional[int] = None, offset: Optional[int] = None) -> List[str]:
        """
        List entities by their URNs bundler method.

        :param limit: The maximum number of entities to return.
        :param offset: The number of entities to skip before starting to collect the result set.
        :return: A list of URNs.
        """
        return self.list(limit=limit, offset=offset)
    
    def list(self, limit: Optional[int] = None, offset: Optional[int] = None) -> List[str]:
        """
        List entities by their URNs.

        :param limit: The maximum number of entities to return.
        :param offset: The number of entities to skip before starting to collect the result set.
        :return: A list of URNs.
        """
        raise NotImplementedError("Subclasses of the Entity class must implement this method.")

    def get_identifier(self, identifier: str) -> str:
        """
        Get the URN of an entity given its URN or UUID.

        :param identifier: The URN or UUID of the entity.
        :return: The URN of the entity.
        """
        if is_valid_uuid(identifier):
            return f"urn:{self.name}:{identifier}"
        elif identifier.startswith(f"urn:{self.name}:"):
            return identifier
        else:
            raise ValueError(f"Invalid identifier: {identifier}")

    def cache(self, urn: str, obj) -> None:
        """
        Cache the entity.

        This method caches the entity for faster access. 
        """
        if config.settings.get("CACHE_ENABLED", False):
            try:
                REDIS.set(urn, obj)        
            except Exception as e:
                logging.error(f"Failed to cache entity {urn}: {e}")

    def invalidate_cache(self, urn: str) -> None:
        """
        Invalidate the cache for the entity.

        :param urn: The URN of the entity.
        """
        if config.settings.get("CACHE_ENABLED", False):
            try:
                REDIS.delete(urn)
            except Exception as e:
                logging.error(f"Failed to invalidate cache for entity {urn}: {e}")

    def get_cached(self, urn: str) -> Optional[Dict[str, Any]]:
        """
        Get the cached entity by its URN.

        :param urn: The URN of the entity.
        :return: The cached entity or None if not found.
        """
        obj = None
        if config.settings.get("CACHE_ENABLED", False):
            try:
                # Cache hit if no exception and obj is not None
                obj = REDIS.get(urn)
            except Exception as e:
                logging.error(f"Failed to get cached entity {urn}: {e}")
        # Cache miss or caching disabled
        if obj is None:
            obj = self.get(urn)
            self.cache(urn, obj)
        return obj

    def get_entity(self, urn: str) -> Dict[str, Any]:
        """
        Get an entity by its URN or UUID bundler method.

        :param urn: The URN or UUID of the entity to fetch.
        :return: The entity or None if not found.
        """
        identifier = self.get_identifier(urn)
        return self.get_cached(identifier)

   
    def get(self, urn: str) -> Dict[str, Any]:
        """
        Get an entity by its URN or UUID.

        :param urn: The URN of the entity to fetch.
        :return: The entity or None if not found.
        """
        raise NotImplementedError("Subclasses of the Entity class must implement this method.")

    def create_entity(self, spec) -> Dict[str, Any]:
        """
        Create a new entity bundler method.

        :param data: The data for the new entity.
        :return: The created entity.
        """
        return self.create(spec)

    def create(self, spec) -> Dict[str, Any]:
        """
        Create a new entity.

        :param data: The validated data for the new entity.
        :return: The created entity.
        """
        raise NotImplementedError("Subclasses of the Entity class must implement this method.")

    def delete_entity(self, urn: str, purge=False) -> bool:
        """
        Delete an entity by its URN or UUID bundler method.

        :param urn: The URN or UUID of the entity to delete.
        :param purge: Whether to permanently delete the entity.
        :return: True if the entity was deleted, False otherwise.
        """
        identifier = self.get_identifier(urn)
        self.invalidate_cache(identifier)
        return self.delete(identifier, purge=purge)

    def delete(self, urn: str, purge=False) -> bool:
        """
        Delete an entity by its URN.

        :param urn: The URN of the entity to delete.
        :param purge: Whether to permanently delete the entity.
        :return: True if the entity was deleted, False otherwise.
        """
        raise NotImplementedError("Subclasses of the Entity class must implement this method.")

    def patch_entity(self, urn: str, spec) -> Dict[str, Any]:
        """
        Update an entity by its URN or UUID bundler method.

        :param urn: The URN or UUID of the entity to update.
        :param data: The data to update the entity with.
        :return: The updated entity.
        """
        if self.update_schema is None:
            raise NotAllowedError(f"The {self.name} entity does not support updates.")
        
        identifier = self.get_identifier(urn)
        self.invalidate_cache(identifier)
        return self.patch(identifier, spec)

    def patch(self, urn: str, spec) -> Dict[str, Any]:
        """
        Update an entity by its URN.

        :param urn: The URN of the entity to update.
        :param data: The validated data to update the entity with.
        :return: The updated entity.
        """
        raise NotImplementedError("Subclasses of the Entity class must implement this method.")

    def search_entities(self, query: str, limit: Optional[int] = None, offset: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Search for entities bundler method.

        :param query: The search query.
        :param limit: The maximum number of entities to return.
        :param offset: The number of entities to skip before starting to collect the result set.
        :return: A list of entities matching the search query.
        """
        return self.search(query=query, limit=limit, offset=offset)

    def search(self, query: str, limit: Optional[int] = None, offset: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Search for entities.

        :param query: The search query.
        :param limit: The maximum number of entities to return.
        :param offset: The number of entities to skip before starting to collect the result set.
        :return: A list of entities matching the search query.
        """
        raise NotImplementedError("Subclasses of the Entity class must implement this method.")
    



