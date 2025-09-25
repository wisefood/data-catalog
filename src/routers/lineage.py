from fastapi import APIRouter
from ..models import LineageEdge
from ..service import stores

router = APIRouter(prefix="/v1/lineage", tags=["Lineage Operations"])

@router.post("")
def add_lineage(edge: LineageEdge):
    stores.add_relation(edge.from_urn, edge.to_urn, edge.relation)
    return {"ok": True}
