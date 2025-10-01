from fastapi import APIRouter, Request, Depends 
from routers.generic import render
from auth import auth
from schemas import LoginSchema
import kutils
from exceptions import AuthenticationError
router = APIRouter(prefix="/api/v1/system", tags=["System Operations"])

@router.get("/ping")
@render()
def ping(request: Request):
    return "pong"

@router.post("/login")
@render()
def login(request: Request, creds: LoginSchema):
    return kutils.get_token(username=creds.username, password=creds.password)