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
from main import config


class ElasticsearchClientSingleton:
    """Singleton class that holds a pool of Elasticsearch clients."""

    _pool = []
    _counter = 0
    _lock = threading.Lock()

    def get_client(self) -> Elasticsearch:
        """Ensure pool is initialized and return one Elasticsearch client (round robin)."""
        if not self._pool:
            self._initialize_elasticsearch()
        pool_item = self._select_pool_item()
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
        es = Elasticsearch(hosts=config.settings["ELASTIC_HOST"])
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
                index="organizations",
                body=organization_index(config.settings["ES_DIM"]),
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
            client = Elasticsearch(hosts=config.settings["ELASTIC_HOST"])
            cls._pool.append(client)
        cls._bootstrap()

    def index_exists(self, index_name: str) -> bool:
        client = self.get_client()
        return client.indices.exists(index=index_name)

    def get_entity(self, index_name: str, urn: str):
        client = self.get_client()
        try:
            r = client.get(index=index_name, id=urn)
            return r["_source"]
        except Exception:
            return None

    def list_entities(
        self, index_name: str, size: int = 1000, offset: int = 0
    ) -> list[str]:
        client = self.get_client()
        body = {
            "from": offset,
            "size": size,
            "_source": False,
            "query": {"bool": {"must_not": {"term": {"status": "deleted"}}}},
        }
        r = client.search(index=index_name, body=body, _source_includes=["_id"])
        return [h["_id"] for h in r["hits"]["hits"]]

    def fetch_entities(self, index_name: str, limit: int, offset: int) -> list[dict]:
        """
        Fetch entity representations from an Elasticsearch index
        using offset + limit pagination.

        Args:
            index_name: name of the ES index
            limit: number of entities to return
            offset: starting offset for pagination

        Returns:
            List of entity documents (_source only).
        """
        client = self.get_client()
        body = {
            "from": offset,
            "size": limit,
            "query": {"bool": {"must_not": {"term": {"status": "deleted"}}}},
        }
        r = client.search(
            index=index_name,
            body=body,
        )
        return [hit["_source"] for hit in r["hits"]["hits"]]

    def index_entity(self, index_name: str, document: dict):
        client = self.get_client()
        client.index(
            index=index_name, id=document["urn"], document=document, refresh="wait_for"
        )

    def delete_entity(self, index_name: str, urn: str):
        client = self.get_client()
        client.delete(index=index_name, id=urn, refresh="wait_for")

    def update_entity(self, index_name: str, document: dict):
        client = self.get_client()
        client.update(
            index=index_name, id=document["urn"], doc=document, refresh="wait_for"
        )

    def search_entities(self, index_name: str, qspec):
        client = self.get_client()
        r = client.search(index=index_name, body=qspec)
        return [h["_source"] for h in r["hits"]["hits"]]


ELASTIC_CLIENT = ElasticsearchClientSingleton()
