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
from backend.elastic import ELASTIC_CLIENT
from datetime import datetime
from utils import is_valid_uuid
from main import config
import uuid
from exceptions import (
    NotAllowedError,
    DataError,
    InternalError,
    NotFoundError,
    ConflictError,
)
import logging
from schemas import GuideCreationSchema, GuideUpdateSchema, GuideSchema, SearchSchema


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

    OPERATIONS = frozenset(
        [
            "list",
            "fetch",
            "get",
            "create",
            "delete",
            "search",
            "patch",
        ]
    )

    def __init__(
        self,
        name: str,
        collection_name: str,
        dump_schema: BaseModel,
        creation_schema: BaseModel,
        update_schema: BaseModel,
    ):
        """
        Initialize the entity with its name, collection name, creation schema, and update schema.

        :param name: The name of the entity.
        :param collection_name: The name of the collection of such entities.
        :param creation_schema: The schema used for creating instances of this entity.
        :param update_schema: The schema used for updating instances of this entity.
        """
        self.name = name
        self.collection_name = collection_name
        self.dump_schema = dump_schema
        self.creation_schema = creation_schema
        self.update_schema = update_schema

        self.operations = Entity.OPERATIONS.copy()
        if update_schema is None:
            self.operations.remove("patch")

    def fetch_entities(
        self, limit: Optional[int] = None, offset: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch a list of entities bundler method.

        :param limit: The maximum number of entities to return.
        :param offset: The number of entities to skip before starting to collect the result set.
        :return: A list of entities.
        """
        return self.fetch(limit=limit, offset=offset)

    def fetch(
        self, limit: Optional[int] = None, offset: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch a list of entities.

        :param limit: The maximum number of entities to return.
        :param offset: The number of entities to skip before starting to collect the result set.
        :return: A list of entities.
        """
        raise NotImplementedError(
            "Subclasses of the Entity class must implement this method."
        )

    def list_entities(
        self, limit: Optional[int] = None, offset: Optional[int] = None
    ) -> List[str]:
        """
        List entities by their URNs bundler method.

        :param limit: The maximum number of entities to return.
        :param offset: The number of entities to skip before starting to collect the result set.
        :return: A list of URNs.
        """
        return self.list(limit=limit, offset=offset)

    def list(
        self, limit: Optional[int] = None, offset: Optional[int] = None
    ) -> List[str]:
        """
        List entities by their URNs.

        :param limit: The maximum number of entities to return.
        :param offset: The number of entities to skip before starting to collect the result set.
        :return: A list of URNs.
        """
        raise NotImplementedError(
            "Subclasses of the Entity class must implement this method."
        )

    def get_identifier(self, identifier: str) -> str:
        """
        Get the URN of an entity given its URN or UUID.

        :param identifier: The URN or UUID of the entity.
        :return: The URN of the entity.
        """
        if is_valid_uuid(identifier):
            return self.resolve_urn(identifier)
        elif identifier.startswith(f"urn:{self.name}:"):
            return identifier
        else:
            return f"urn:{self.name}:{identifier}"

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

    def resolve_urn(self, uuid: str) -> str:
        """
        Resolve the URN of an entity given its UUID.
        :param uuid: The UUID of the entity.
        :return: The URN of the entity.
        """
        try:
            qspec = {"query": {"term": {"id": uuid}}}
            entity = ELASTIC_CLIENT.search_entities(
                index_name=self.collection_name, qspec=qspec
            )
            if not entity:
                raise NotFoundError(f"Guide with UUID {uuid} not found.")
            return entity[0]["urn"]
        except Exception as e:
            raise NotFoundError(f"Failed to resolve URN for UUID {uuid}: {e}")

    def get_cached(self, urn: str) -> Optional[Dict[str, Any]]:
        obj = None
        if config.settings.get("CACHE_ENABLED", False):
            try:
                obj = REDIS.get(urn)
            except Exception as e:
                logging.error(f"Failed to get cached entity {urn}: {e}")

        if obj is None:
            obj = self.get(urn)
            self.cache(urn, obj)

        return self.dump_schema.model_validate(obj).model_dump(mode="json")

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
        raise NotImplementedError(
            "Subclasses of the Entity class must implement this method."
        )

    def create_entity(self, spec, creator) -> Dict[str, Any]:
        """
        Create a new entity bundler method.

        :param spec: The data for the new entity.
        :param creator: The dict of the creator user fetched from header.
        :return: The created entity.
        """
        self.create(spec, creator)
        return self.get_entity(spec["urn"])

    def create(self, spec, creator) -> None:
        """
        Create a new entity.

        :param data: The validated data for the new entity.
        :param creator: The creator user dict fetched from header.
        :return: The created entity.
        """
        raise NotImplementedError(
            "Subclasses of the Entity class must implement this method."
        )

    def delete_entity(self, urn: str) -> bool:
        """
        Delete an entity by its URN or UUID bundler method.

        :param urn: The URN or UUID of the entity to delete.
        :return: True if the entity was deleted, False otherwise.
        """
        identifier = self.get_identifier(urn)
        self.invalidate_cache(identifier)
        return self.delete(identifier)

    def delete(self, urn: str, purge=False) -> bool:
        """
        Delete an entity by its URN.

        :param urn: The URN of the entity to delete.
        :param purge: Whether to permanently delete the entity.
        :return: True if the entity was deleted, False otherwise.
        """
        raise NotImplementedError(
            "Subclasses of the Entity class must implement this method."
        )

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
        self.patch(identifier, spec)
        return self.get_entity(identifier)

    def patch(self, urn: str, spec) -> None:
        """
        Update an entity by its URN.

        :param urn: The URN of the entity to update.
        :param data: The validated data to update the entity with.
        :return: The updated entity.
        """
        raise NotImplementedError(
            "Subclasses of the Entity class must implement this method."
        )

    def search_entities(
        self,
        query: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """
        Search for entities bundler method.

        :param query: The search query.
        :param limit: The maximum number of entities to return.
        :param offset: The number of entities to skip before starting to collect the result set.
        :return: A list of entities matching the search query.
        """
        return self.search(query=query)

    def search(
        self,
        query: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """
        Search for entities.

        :param query: The search query.
        :param limit: The maximum number of entities to return.
        :param offset: The number of entities to skip before starting to collect the result set.
        :return: A list of entities matching the search query.
        """
        raise NotImplementedError(
            "Subclasses of the Entity class must implement this method."
        )

    def upsert_system_fields(self, spec: Dict, update=False) -> Dict[str, Any]:
        """
        Upsert system fields for the entity.

        :param data: The data to upsert system fields into.
        :return: The data with upserted system fields.
        """
        # Fix URN and UUIDs
        if "urn" in spec and not update:
            spec["urn"] = f"urn:{self.name}:{spec['urn'].split(':')[-1]}"
            spec["id"] = str(uuid.uuid4())

        if update and "creator" in spec:
            spec.pop("creator")
        # Generate timestamps
        spec["updated_at"] = str(datetime.now().isoformat())
        if not update:
            spec["created_at"] = str(datetime.now().isoformat())
        return spec


class Guide(Entity):
    def __init__(self):
        super().__init__(
            "guide", "guides", GuideSchema, GuideCreationSchema, GuideUpdateSchema
        )

    def list(
        self, limit: Optional[int] = None, offset: Optional[int] = None
    ) -> List[str]:
        return ELASTIC_CLIENT.list_entities(
            index_name=self.collection_name, size=limit or 100, offset=offset or 0
        )

    def fetch(
        self, limit: Optional[int] = None, offset: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        return ELASTIC_CLIENT.fetch_entities(
            index_name=self.collection_name, limit=limit or 100, offset=offset or 0
        )
    
    def search(
        self, query: Dict[str, Any],
    ):
        try:
            qspec = SearchSchema.model_validate(query).model_dump(mode="json")
        except Exception as e:
            raise DataError(f"Invalid search query: {e}")

        return ELASTIC_CLIENT.search_entities(index_name=self.collection_name, qspec=qspec)

    def get(self, urn: str) -> Dict[str, Any]:
        entity = ELASTIC_CLIENT.get_entity(index_name=self.collection_name, urn=urn)
        if entity is None:
            raise NotFoundError(f"Guide with URN {urn} not found.")
        return entity

    def create(self, spec: GuideCreationSchema, creator: dict) -> Dict[str, Any]:
        # Validate input data
        try:
            guide_data = self.creation_schema.model_validate(spec)
        except Exception as e:
            raise DataError(f"Invalid data for creating guide: {e}")

        # Check if guide with same URN already exists
        try:
            existing = self.get_entity(urn=guide_data.urn)
            if existing is not None:
                raise ConflictError(f"Guide with URN {guide_data.urn} already exists.")
        except NotFoundError:
            pass  # Expected if guide does not exist

        # Convert to dict and store in Elasticsearch
        guide_dict = guide_data.model_dump(mode="json")
        guide_dict["creator"] = creator["preferred_username"]
        guide_dict = self.upsert_system_fields(guide_dict, update=False)
        try:
            ELASTIC_CLIENT.index_entity(
                index_name=self.collection_name, document=guide_dict
            )
        except Exception as e:
            raise InternalError(f"Failed to create guide: {e}")

    def patch(self, urn, spec):
        try:
            guide_data = self.update_schema.model_validate(spec)
        except Exception as e:
            raise DataError(f"Invalid data for updating guide: {e}")

        # Check if guide exists
        try:
            existing = self.get(urn=urn)
            if existing is None:
                raise NotFoundError(f"Guide with URN {urn} not found.")
        except NotFoundError:
            raise NotFoundError(f"Guide with URN {urn} not found.")

        # Convert to dict and update in Elasticsearch
        guide_dict = guide_data.model_dump(mode="json", exclude_unset=True)
        guide_dict = self.upsert_system_fields(guide_dict, update=True)
        guide_dict["urn"] = urn
        try:
            ELASTIC_CLIENT.update_entity(
                index_name=self.collection_name, document=guide_dict
            )
        except Exception as e:
            raise InternalError(f"Failed to update guide: {e}")

    def delete(self, urn: str) -> bool:
        # Permanently delete the guide
        try:
            ELASTIC_CLIENT.delete_entity(index_name=self.collection_name, urn=urn)
        except Exception as e:
            raise InternalError(f"Failed to delete guide: {e}")

        return {"deleted": urn}


GUIDE = Guide()
