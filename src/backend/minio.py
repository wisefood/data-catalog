import threading
import urllib3
import logging
from minio import MinioAdmin
from minio import Minio
import requests
from main import config
import xml.etree.ElementTree as ET
from minio.error import S3Error
from exceptions import InternalError


logger = logging.getLogger(__name__)

class MinioClientSingleton:
    """
    Lazy‐initialized singleton that holds one Minio + MinioAdmin pair
    sharing a single urllib3.PoolManager.
    """

    _lock = threading.Lock()
    _initialized = False
    client = None  # type: Minio
    admin = None  # type: MinioAdmin

    @classmethod
    def _initialize(
        cls,
        num_pools: int = 5,
        maxsize: int = 20,
        block: bool = True,
        retries: int = 3,
        read_timeout: float = 10.0,
        connect_timeout: float = None,
    ):
        endpoint = config.settings["MINIO_ENDPOINT"].replace("http://", "").replace("https://", "")
        access_key = config.settings["MINIO_ROOT"]
        secret_key = config.settings["MINIO_ROOT_PASSWORD"]
        secure = False
        with cls._lock:
            if cls._initialized:
                return

            # build the shared PoolManager
            timeout = urllib3.Timeout(
                connect=(
                    connect_timeout
                    if connect_timeout is not None
                    else urllib3.Timeout.DEFAULT_TIMEOUT
                ),
                read=read_timeout,
            )

            logger.debug(f"Creating PoolManager with num_pools={num_pools}, maxsize={maxsize}, block={block}")

            pool = urllib3.PoolManager(
                num_pools=num_pools,
                maxsize=maxsize,
                block=block,
                retries=urllib3.Retry(total=retries),
                timeout=timeout,
            )

            # object‐storage client
            cls.client = Minio(
                endpoint=endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=secure,
                http_client=pool,
            )

            # admin client (reusing same pool & credentials provider)
            cls.admin = MinioAdmin(
                endpoint=endpoint,
                credentials=cls.client._provider,
                secure=secure,
                http_client=pool,
            )

            cls._initialized = True

    @classmethod
    def get_client(cls) -> Minio:
        """Returns the initialized Minio client"""
        if not cls._initialized:
            cls._initialize()
        return cls.client

    @classmethod
    def get_admin(cls) -> MinioAdmin:
        """Returns the initialized MinioAdmin client."""
        if not cls._initialized:
            cls._initialize()
        return cls.admin
    
    @classmethod
    def get_personalized_client(cls, token) -> Minio:
        """Returns a Minio client for a specific user given their token."""
 
        endpoint = config.settings["MINIO_ENDPOINT"].replace("http://", "").replace("https://", "")
        req = {
            "Action": "AssumeRoleWithWebIdentity",
            "WebIdentityToken": token,
            "Version": "2011-06-15",
            "DurationSeconds": "3600",
        }
        try:
            response = requests.post(url=f"{config.settings['MINIO_ENDPOINT']}", params=req)
        except Exception as e:
            logger.error(f"Failed to get personal Minio client: {e}")
            raise InternalError("Failed to get personal Minio client:", e) from e

        try:
            if response.status_code in range(200, 300):
                # Parse the XML response
                root = ET.fromstring(response.text)

                # Extracting relevant information from the XML
                credentials = root.find(
                    ".//{https://sts.amazonaws.com/doc/2011-06-15/}Credentials"
                )
                if credentials is not None:
                    access_key = (
                        credentials.find(
                            "{https://sts.amazonaws.com/doc/2011-06-15/}AccessKeyId"
                        ).text
                        if credentials.find(
                            "{https://sts.amazonaws.com/doc/2011-06-15/}AccessKeyId"
                        )
                        is not None
                        else None
                    )
                    secret_key = (
                        credentials.find(
                            "{https://sts.amazonaws.com/doc/2011-06-15/}SecretAccessKey"
                        ).text
                        if credentials.find(
                            "{https://sts.amazonaws.com/doc/2011-06-15/}SecretAccessKey"
                        )
                        is not None
                        else None
                    )
                    session_token = (
                        credentials.find(
                            "{https://sts.amazonaws.com/doc/2011-06-15/}SessionToken"
                        ).text
                        if credentials.find(
                            "{https://sts.amazonaws.com/doc/2011-06-15/}SessionToken"
                        )
                        is not None
                        else None
                    )
                    return Minio(
                        endpoint,
                        access_key=access_key,
                        secret_key=secret_key,
                        session_token=session_token,
                        secure=False,
                    )
        except ET.ParseError as e:
            raise InternalError("Failed to parse XML:", e) from e

MINIO_ADMIN = MinioClientSingleton.get_admin()
MINIO_CLIENT = MinioClientSingleton.get_client()
MINIO = MinioClientSingleton