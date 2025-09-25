from fastapi import APIRouter
from ..models import LineageEdge
from ..service import stores

router = APIRouter(prefix="/v1/system", tags=["System Operations"])

@router.get("/ping")
def ping():
    return {"pong": True}
