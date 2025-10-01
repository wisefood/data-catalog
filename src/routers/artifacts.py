from fastapi import APIRouter, Request, Depends, UploadFile, Form, File
from routers.generic import render
from schemas import ArtifactCreationSchema, ArtifactUpdateSchema
import kutils
from exceptions import DataError
from entity import ARTIFACT
from auth import auth
import logging

logger = logging.getLogger(__name__)

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
async def api_upload_artifact(
    request: Request,
    file: UploadFile = File(...),
    parent_urn: str = Form(...),
    title: str = Form(None),
    description: str = Form(None),
    language: str = Form(None),
):
    """
    Upload a file and create an artifact.
    
    Max file size: 1GB
    """
    MAX_FILE_SIZE = 1_073_741_824
    
    # Read file content
    file_content = await file.read()
    file_size = len(file_content)
    
    if file_size > MAX_FILE_SIZE:
        raise DataError(f"File size ({file_size} bytes) exceeds maximum allowed size of 1GB")
    
    if file_size == 0:
        raise DataError("File is empty")
    
    # Upload to MinIO and create artifact
    artifact_data = await ARTIFACT.upload(
        file=file,
        file_content=file_content,
        parent_urn=parent_urn,
        title=title,
        description=description,
        language=language,
        creator=kutils.current_user(request),
        token=kutils.current_token(request)
    )
    
    return artifact_data

@router.patch("/{id}", dependencies=[Depends(auth())])
@render()
def api_patch_artifact(request: Request, id: str, a: ArtifactUpdateSchema):
    return


@router.delete("/{id}", dependencies=[Depends(auth())])
@render()
def api_delete_artifact(request: Request, id: str):
    return
