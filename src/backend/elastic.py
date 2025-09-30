import threading
from elasticsearch import Elasticsearch
from es_schema import (
    recipe_collection_index,
    paper_index,
    guide_index,
    policy_index,
    organization_index,
    person_index,
)
from ..main import config


class ElasticsearchClientSingleton:
    """Singleton class that holds a pool of Elasticsearch clients."""

    _pool = []
    _counter = 0
    _lock = threading.Lock()

    @classmethod
    def get_client(cls) -> Elasticsearch:
        """Ensure pool is initialized and return one Elasticsearch client (round robin)."""
        if not cls._pool:
            cls._initialize_elasticsearch()
        pool_item = cls._select_pool_item()
        return pool_item

    @classmethod
    def _select_pool_item(cls):
        with cls._lock:
            index = cls._counter % len(cls._pool)
            cls._counter += 1
            return cls._pool[index]

    @classmethod
    def _bootstrap(cls):
        """Create indices in Elasticsearch if they do not exist."""
        es = Elasticsearch(hosts=config.settings["ELASTIC_HOSTS"])
        if not es.indices.exists(index="recipes"):
            es.indices.create(
                index="recipes", body=recipe_collection_index(config.settings["ES_DIM"])
            )
        if not es.indices.exists(index="guides"):
            es.indices.create(
                index="guides", body=guide_index(config.settings["ES_DIM"])
            )
        if not es.indices.exists(index="policies"):
            es.indices.create(
                index="policies", body=policy_index(config.settings["ES_DIM"])
            )
        if not es.indices.exists(index="papers"):
            es.indices.create(
                index="papers", body=paper_index(config.settings["ES_DIM"])
            )
        if not es.indices.exists(index="organizations"):
            es.indices.create(
                index="organizations", body=organization_index(config.settings["ES_DIM"])
            )
        if not es.indices.exists(index="persons"):
            es.indices.create(
                index="persons", body=person_index(config.settings["ES_DIM"])
            )

    @classmethod
    def _initialize_elasticsearch(cls):
        """Initialize a pool of Elasticsearch clients."""
        pool_size = int(config.settings.get("ELASTICSEARCH_POOL_SIZE", 5))
        for _ in range(pool_size):
            client = Elasticsearch(hosts=config.settings["ELASTIC_HOSTS"])
            cls._pool.append(client)
        cls._bootstrap()


ELASTIC_CLIENT = ElasticsearchClientSingleton.get_client()
