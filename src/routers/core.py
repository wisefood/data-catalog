from fastapi import APIRouter, Request
from routers.generic import render
from exceptions import AuthenticationError
router = APIRouter(prefix="/api/v1/system", tags=["System Operations"])

@router.get("/ping")
@render()
def ping(request: Request):
    return "pong"

@router.get("/error")
@render()
def error(request: Request):
    raise AuthenticationError("This is a test error")
