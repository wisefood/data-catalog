from fastapi import APIRouter
from ..models import RecipeIn
from ..service import stores

router = APIRouter(prefix="/v1/recipes", tags=["Recipe Operations"])

@router.get("/search")
def search_recipes(q: str):
    return stores.search(q, index="recipes")

@router.post("/semantic-search")
def semantic_recipes(q: str):
    return stores.semantic_search(q, index="recipes")

@router.get("/{urn}")
def get_recipe(urn: str):
    return stores.get_recipe(urn)

@router.post("")
def add_recipe(r: RecipeIn):
    return stores.upsert_recipe(r.model_dump())
