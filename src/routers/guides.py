from fastapi import APIRouter, Request, Depends
from schemas import GuideCreationSchema, GuideUpdateSchema, SearchSchema
from routers.generic import render
import kutils
from auth import auth
from entity import GUIDE
router = APIRouter(prefix="/api/v1/guides", tags=["Dietary Guides Operations"])


@router.get("/", dependencies=[Depends(auth())])
@render()
def api_list_guides(request: Request, limit: int = 100, offset: int = 0):
    return GUIDE.list_entities(limit=limit, offset=offset)

@router.get(".fetch", dependencies=[Depends(auth())])
@render()
def api_fetch_guides(request: Request, limit: int = 100, offset: int = 0):
    return GUIDE.fetch_entities(limit=limit, offset=offset)

@router.post("/search", dependencies=[Depends(auth())])
@render()
def api_search_guides(request: Request, q: SearchSchema):
    return GUIDE.search_entities(query=q)

@router.get("/{urn}", dependencies=[Depends(auth())])
@render()
def api_get_guide(request: Request, urn: str):
    return GUIDE.get_entity(urn)

@router.post("/", dependencies=[Depends(auth())])
@render()
def api_create_guide(request: Request, g: GuideCreationSchema):
    return GUIDE.create_entity(g.model_dump(mode="json"), creator=kutils.current_user(request))

@router.patch("/{urn}", dependencies=[Depends(auth())])
@render()
def api_patch_guide(request: Request, urn: str, g: GuideUpdateSchema):
    return GUIDE.patch_entity(urn, g.model_dump(mode="json", exclude_unset=True))

@router.delete("/{urn}", dependencies=[Depends(auth())])
@render()
def api_delete_guide(request: Request, urn: str):
    return GUIDE.delete_entity(urn)