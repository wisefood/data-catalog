from fastapi import APIRouter
from ..models import LineageEdge
from ..service import stores

router = APIRouter(prefix="/api/v1/system", tags=["System Operations"])

@router.get("ping")
def ping():
    return {"ok": True}
