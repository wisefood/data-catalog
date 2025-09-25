from fastapi import FastAPI
from .service import stores

# create FastAPI app
api = FastAPI(title="WiseFood Catalog API")

# initialize store once
stores.bootstrap()

# import routers
from .routers import recipes, guides, policies, lineage

api.include_router(recipes.router)
api.include_router(guides.router)
api.include_router(policies.router)
api.include_router(lineage.router)
