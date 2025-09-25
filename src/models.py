from pydantic import BaseModel
from typing import List, Optional

class APIResponse (BaseModel):
    result: 
    

class Ingredient(BaseModel):
    name: str
    quantity: Optional[str] = None
    unit: Optional[str] = None

class Nutrition(BaseModel):
    calories: Optional[float] = None
    protein: Optional[float] = None
    carbs: Optional[float] = None
    fat: Optional[float] = None

class RecipeIn(BaseModel):
    urn: str
    title: str
    description: Optional[str] = None
    ingredients: List[Ingredient] = []
    instructions: Optional[str] = None
    tags: List[str] = []
    nutrition: Optional[Nutrition] = None
    embedding_hint: Optional[str] = None
    embedding: Optional[list[float]] = None

class GuideIn(BaseModel):
    urn: str
    title: str
    content: str
    topic: Optional[str] = None

class PolicyIn(BaseModel):
    urn: str
    title: str
    content: str
    authority: Optional[str] = None
    effective_date: Optional[str] = None

class LineageEdge(BaseModel):
    from_urn: str
    to_urn: str
    relation: str
