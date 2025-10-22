"""
Organization Entity
------------------
The Organization entity inherits from the base Entity class and provides
methods to manage organization data, including retrieval, creation,
updating, and deletion of organizations. Collection operations such as
LIST, FETCH and SEARCH are implemented in the parent class. This class 
consolidates and applies schemas specific to organizations for data validation
and serialization. It implements the CRUD operations while leveraging
the underlying infrastructure provided by the Entity base class.
"""
from typing import Optional, List, Dict, Any
from backend.elastic import ELASTIC_CLIENT
from exceptions import (
    DataError,
    InternalError,
    NotFoundError,
    ConflictError,
)
import logging
from schemas import (
    OrganizationCreationSchema,
    OrganizationUpdateSchema,
    OrganizationSchema,
    SearchSchema,
)

from entity import Entity

logger = logging.getLogger(__name__)


class Organization(Entity):
    def __init__(self):
        super().__init__(
            "organization",
            "organizations",
            OrganizationSchema,
            OrganizationCreationSchema,
            OrganizationUpdateSchema,
        )

    def get(self, urn: str) -> OrganizationSchema:
        """Retrieve an organization by its ID."""
        entity = ELASTIC_CLIENT.get_entity(index_name=self.collection_name, urn=urn)
        if entity is None:
            raise NotFoundError(f"Organization with URN {urn} not found.")
        return entity

    def create(self, spec: OrganizationCreationSchema, creator=None) -> Dict[str, Any]:
        """Create a new organization."""
        try:
            org_data = self.creation_schema.model_validate(spec)
        except Exception as e:
            raise DataError(f"Invalid organization creation spec: {e}")

        try:
            self.validate_existence("urn:organization:" + org_data.urn)

            raise ConflictError(f"Organization with URN {org_data.urn} already exists.")
        except NotFoundError:
            pass  # Expected, continue to create

        org_dict = org_data.model_dump(mode="json")
        org_dict = self.upsert_system_fields(org_dict, update=False)

        try:
            ELASTIC_CLIENT.index_entity(
                index_name=self.collection_name, document=org_dict
            )
        except Exception as e:
            raise InternalError(f"Failed to create organization: {e}")

    def patch(self, urn: str, spec: OrganizationUpdateSchema) -> Dict[str, Any]:
        """Partially update an existing organization."""
        try:
            org_data = self.update_schema.model_validate(spec)
        except Exception as e:
            raise DataError(f"Invalid organization update spec: {e}")

        # Check if org exists, URN is normalized here
        self.validate_existence(urn)

        # Convert to dict and update in Elasticsearch
        org_dict = org_data.model_dump(
            mode="json", exclude_unset=True, exclude_none=True
        )
        org_dict = self.upsert_system_fields(org_dict, update=True)
        org_dict["urn"] = urn  # Ensure URN is included

        try:
            ELASTIC_CLIENT.update_entity(
                index_name=self.collection_name, document=org_dict
            )
        except Exception as e:
            raise InternalError(f"Failed to update organization: {e}")

    def delete(self):
        raise NotImplementedError("Organization deletion is not supported.")


ORGANIZATION = Organization()
