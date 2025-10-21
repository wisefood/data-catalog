from fastapi import APIRouter, Request, Depends
from schemas import OrganizationCreationSchema, OrganizationUpdateSchema, SearchSchema
from routers.generic import render
import kutils
from auth import auth
from entities.organizations import ORGANIZATION

router = APIRouter(prefix="/api/v1/organizations", tags=["Organization Management Operations"])


@router.get(
    "",
    dependencies=[Depends(auth())],
    summary="List organizations",
    description="Retrieve a paginated list of organizations from the database."
)
@render()
def api_list_organizations(request: Request, limit: int = 100, offset: int = 0):
    return ORGANIZATION.list_entities(limit=limit, offset=offset)

@router.get(
    "/fetch",
    dependencies=[Depends(auth())],
    summary="Fetch organizations",
    description="Fetch a paginated collection of organizations with detailed information."
)
@render()
def api_fetch_organizations(request: Request, limit: int = 100, offset: int = 0):
    return ORGANIZATION.fetch_entities(limit=limit, offset=offset)



@router.get("/{urn}", dependencies=[Depends(auth())], summary="Get organization by URN")
@render()
def api_get_organization_by_urn(request: Request, urn: str):
    return ORGANIZATION.get_entity(urn)


@router.post(
    "",
    dependencies=[Depends(auth())],
    summary="Create a new organization",
    description="Create a new organization in the database."
)
@render()
def api_create_organization(request: Request, o: OrganizationCreationSchema):
    return ORGANIZATION.create_entity(spec=o.model_dump(mode="json"), creator=None)

@router.post(
    "/search",
    dependencies=[Depends(auth())],
    summary="Search organizations",
    description="Search for organizations based on specified criteria."
)
@render()
def api_search_organizations(request: Request, q: SearchSchema):
    return ORGANIZATION.search_entities(query=q)


@router.patch("/{urn}",
              dependencies=[Depends(auth())],
              summary="Update an existing organization",
              description="Partially update an existing organization in the database.")
@render()
def api_patch_organization(request: Request, urn: str, o: OrganizationUpdateSchema):
    return ORGANIZATION.patch_entity(urn=urn, spec=o.model_dump(mode="json", exclude_unset=True))