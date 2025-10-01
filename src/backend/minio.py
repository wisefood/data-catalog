import subprocess
import threading
import urllib3
import logging
import json
from minio import MinioAdmin
from minio import Minio
from minio.credentials.providers import StaticProvider

from main import config

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
                endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=secure,
                http_client=pool,
            )

            # admin client (reusing same pool & credentials provider)
            cls.admin = MinioAdmin(
                endpoint,
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

MINIO_ADMIN = MinioClientSingleton.get_admin
MINIO_CLIENT = MinioClientSingleton.get_client