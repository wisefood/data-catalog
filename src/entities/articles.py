"""
Article Entity
------------------
The Article entity inherits from the base Entity class and provides
methods to manage organization data, including retrieval, creation,
updating, and deletion of scientific articles. Collection operations such as
LIST, FETCH and SEARCH are implemented in the parent class. This class 
consolidates and applies schemas specific to scientific articles for data validation
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
    SearchSchema,
)

from entity import Entity

logger = logging.getLogger(__name__)


class Article(Entity):
    def __init__(self):
        super().__init__(
            "article",
            "articles",
        )
    def get(self, urn: str) -> Dict[str, Any]:
        pass

    def create(self, spec: Dict[str, Any], creator=None) -> Dict[str, Any]:
        pass

    def patch(self, urn: str, spec: Dict[str, Any], updater=None) -> Dict[str, Any]:
        pass

    def delete(self, urn: str) -> bool:
        pass 
