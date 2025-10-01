from fastapi import APIRouter, Request, Depends
from routers.generic import render
import kutils
from auth import auth
router = APIRouter(prefix="/api/v1/artifacts", tags=["Artifact Management Operations"])



@router.get("/{urn}", dependencies=[Depends(auth())])
@render()
def api_get_artifact(request: Request, urn: str):
    return 