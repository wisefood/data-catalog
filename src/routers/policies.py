from fastapi import APIRouter
from ..models import PolicyIn
from ..service import stores

router = APIRouter(prefix="/v1/policies", tags=["Policy Operations"])

# static first
@router.get("/search")
def search_policies(q: str):
    return stores.search(q, index="policies")

@router.post("/semantic-search")
def semantic_policies(q: str):
    return stores.semantic_search(q, index="policies")

# dynamic after
@router.get("/{urn}")
def get_policy(urn: str):
    return stores.get_policy(urn)

@router.post("")
def add_policy(p: PolicyIn):
    return stores.upsert_policy(p.model_dump())
