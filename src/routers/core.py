from fastapi import APIRouter, Request, Depends 
from routers.generic import render
from auth import auth
from exceptions import AuthenticationError
router = APIRouter(prefix="/api/v1/system", tags=["System Operations"])

@router.get("/ping")
@render()
def ping(request: Request):
    return "pong"

@router.get("/error", dependencies=[Depends(auth())])
@render()
def error(request: Request):
    raise AuthenticationError("This is a test error")
