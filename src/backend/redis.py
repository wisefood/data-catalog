import redis
from main import config

from exceptions import (
    BadGatewayError,
    NotFoundError,
)
import traceback


class RedisClient:
    _pool = None

    @classmethod
    def _initialize_redis(cls):
        cls._pool = redis.ConnectionPool(
            host=config.settings["REDIS_HOST"],
            port=config.settings["REDIS_PORT"],
            db=1,
            decode_responses=True,
            max_connections=10,
        )
        return cls._pool

    def set(self, key, value):
        """Set a value in Redis using a connection from the pool."""
        try:
            if self._pool is None:
                self._initialize_redis()
            conn = redis.Redis(connection_pool=self._pool)
            conn.set(key, value)
        except Exception as e:
            raise BadGatewayError("Redis error", detail=traceback.format_exc())

    def get(self, key):
        """Get a value from Redis using a connection from the pool."""
        try:
            if self._pool is None:
                self._initialize_redis()
            conn = redis.Redis(connection_pool=self._pool)
            value = conn.get(key)
            return value
        except Exception as e:
            raise BadGatewayError("Redis error", detail=traceback.format_exc())

    def delete(self, key):
        """Delete a value from Redis using a connection from the pool."""
        try:
            if self._pool is None:
                self._initialize_redis()
            conn = redis.Redis(connection_pool=self._pool)
            conn.delete(key)
        except Exception as e:
            raise BadGatewayError("Redis error", detail=traceback.format_exc())


# Create a singleton instance of RedisClient
REDIS = RedisClient()