from fastapi import APIRouter, Request, Depends, UploadFile
from routers.generic import render
from schemas import ArtifactSchema, ArtifactCreationSchema, ArtifactUpdateSchema
import kutils
from backend.minio import MINIO
from entity import ARTIFACT
from auth import auth

router = APIRouter(prefix="/api/v1/artifacts", tags=["Artifact Management Operations"])


@router.get("/{id}", dependencies=[Depends(auth())])
@render()
def api_get_artifact(request: Request, id: str):
    return ARTIFACT.get_entity(id)


@router.get("/{id}/download", dependencies=[Depends(auth())])
@render()
def api_download_artifact(request: Request, id: str):
    return


@router.post("/", dependencies=[Depends(auth())])
@render()
def api_create_artifact(request: Request, a: ArtifactCreationSchema):
    return ARTIFACT.create_entity(
        a.model_dump(mode="json"), kutils.current_user(request)
    )


@router.post("/upload", dependencies=[Depends(auth())])
@render()
def api_upload_artifact(request: Request, file: UploadFile):
    return


@router.patch("/{id}", dependencies=[Depends(auth())])
@render()
def api_patch_artifact(request: Request, id: str, a: ArtifactUpdateSchema):
    return


@router.delete("/{id}", dependencies=[Depends(auth())])
@render()
def api_delete_artifact(request: Request, id: str):
    return
