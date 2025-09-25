from fastapi import APIRouter
from ..models import GuideIn
from ..service import stores

router = APIRouter(prefix="/v1/guides", tags=["Guide Operations"])

# static first
@router.get("/search")
def search_guides(q: str):
    return stores.search(q, index="guides")

@router.post("/semantic-search")
def semantic_guides(q: str):
    return stores.semantic_search(q, index="guides")

# dynamic after
@router.get("/{urn}")
def get_guide(urn: str):
    return stores.get_guide(urn)

@router.post("")
def add_guide(g: GuideIn):
    return stores.upsert_guide(g.model_dump())
