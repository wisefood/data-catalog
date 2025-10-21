# -----------------------------------
#
#  Organization Entity
#
# -----------------------------------
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

    def create(self, spec: OrganizationCreationSchema, creator = None) -> Dict[str, Any]:
        """Create a new organization."""
        try:
            org_data = self.creation_schema.model_validate(spec)
        except Exception as e:
            raise DataError(f"Invalid organization creation spec: {e}")

        try:
            self.validate_existence("urn:organization:"+ org_data.urn)

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

    def list(self, limit: Optional[int] = None, offset: Optional[int] = None) -> List[str]:
        """List organizations with pagination."""
        return ELASTIC_CLIENT.list_entities(
            index_name=self.collection_name, size=limit or 100, offset=offset or 0
        )
    

    def fetch(self, limit: Optional[int] = None, offset: Optional[int] = None) -> List[Dict[str, Any]]:
        """Fetch organizations with detailed information and pagination."""
        return ELASTIC_CLIENT.fetch_entities(
            index_name=self.collection_name, limit=limit or 100, offset=offset or 0
        )
    
    def patch(self, urn: str, spec: OrganizationUpdateSchema) -> Dict[str, Any]:
        """Partially update an existing organization."""
        try:
            org_data = self.update_schema.model_validate(spec)
        except Exception as e:
            raise DataError(f"Invalid organization update spec: {e}")

        # Check if org exists
        self.validate_existence("urn:organization:"+ urn)
 
        # Convert to dict and update in Elasticsearch
        org_dict = org_data.model_dump(mode="json")
        org_dict = self.upsert_system_fields(org_dict, update=True)
        org_dict["urn"] = urn  # Ensure URN is included

        try:
            ELASTIC_CLIENT.index_entity(
                index_name=self.collection_name, document=org_dict
            )
        except Exception as e:
            raise InternalError(f"Failed to update organization: {e}")
        
    def search(self, query: Dict[str, Any]):
        """Search for organizations based on query parameters."""
        try:
            qspec = SearchSchema.model_validate(query).model_dump(mode="json")
        except Exception as e:
            raise DataError(f"Invalid search query: {e}")
        
        return ELASTIC_CLIENT.search_entities(
            index_name=self.collection_name, qspec=qspec
        )
    
    def delete(self):
        raise NotImplementedError("Organization deletion is not supported.")
    

ORGANIZATION = Organization()