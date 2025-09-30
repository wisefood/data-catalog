from fastapi import APIRouter, Request, Depends
from schemas import GuideCreationSchema
from routers.generic import render
from auth import auth
from entity import GUIDE
router = APIRouter(prefix="/api/v1/guides", tags=["Nutrional Guide Operations"])

@router.get("/{urn}")
@render()
def get_guide(urn: str, request: Request, dependencies=[Depends(auth())]):
    return GUIDE.get_entity(urn)

@router.post("")
def add_guide(g: GuideCreationSchema):
    return GUIDE.create_entity(g.model_dump())
