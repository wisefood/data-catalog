import numpy as np
from elasticsearch import Elasticsearch
from neo4j import GraphDatabase
from .deps import settings
from .es_schema import recipe_index, guide_index, policy_index
from .graph_schema import BOOTSTRAP_CYPHER

_embedder = None
def embed_text(text: str):
    global _embedder
    if _embedder is None:
        from sentence_transformers import SentenceTransformer
        _embedder = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
    vec = _embedder.encode([text])[0]
    return (vec / np.linalg.norm(vec)).tolist()


class Stores:
    def __init__(self):
        self.es = Elasticsearch(settings.ES_HOST)
        self.driver = GraphDatabase.driver(settings.NEO4J_URI,
                                           auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD))

    def bootstrap(self):
        if not self.es.indices.exists(index="recipes"):
            self.es.indices.create(index="recipes", body=recipe_index(settings.ES_DIM))
        if not self.es.indices.exists(index="guides"):
            self.es.indices.create(index="guides", body=guide_index(settings.ES_DIM))
        if not self.es.indices.exists(index="policies"):
            self.es.indices.create(index="policies", body=policy_index(settings.ES_DIM))

        with self.driver.session() as s:
            for q in BOOTSTRAP_CYPHER:
                try:
                    s.run(q)
                except Exception as e:
                    print(f"Neo4j bootstrap query failed: {q} -> {e}")

    def upsert_recipe(self, doc: dict):
        if not doc.get("embedding"):
            doc["embedding"] = embed_text(doc.get("embedding_hint") or doc["title"])
        self.es.index(index=settings.ES_INDEX, id=doc["urn"], document=doc, refresh="wait_for")
        with self.driver.session() as s:
            s.run("MERGE (r:Recipe {urn:$urn, title:$title})", {"urn": doc["urn"], "title": doc["title"]})
        return doc
    
    def upsert_guide(self, doc: dict):
        if not doc.get("embedding"):
            doc["embedding"] = embed_text(doc.get("embedding_hint") or doc["title"])
        self.es.index(index="guides", id=doc["urn"], document=doc, refresh="wait_for")
        with self.driver.session() as s:
            s.run("MERGE (g:Guide {urn:$urn, title:$title})",
                {"urn": doc["urn"], "title": doc["title"]})
        return doc

    def upsert_policy(self, doc: dict):
        if not doc.get("embedding"):
            doc["embedding"] = embed_text(doc.get("embedding_hint") or doc["title"])
        self.es.index(index="policies", id=doc["urn"], document=doc, refresh="wait_for")
        with self.driver.session() as s:
            s.run("MERGE (p:Policy {urn:$urn, title:$title})",
                {"urn": doc["urn"], "title": doc["title"]})
        return doc

    def search(self, q: str, index: str = "recipes", k: int = 5):
        if q == "*":
            body = {"size": k, "query": {"match_all": {}}}
        else:
            body = {
                "size": k,
                "query": {
                    "multi_match": {
                        "query": q,
                        "fields": [
                            "title^3",
                            "description",
                            "ingredients.name",
                            "tags"
                        ]
                    }
                }
            }
        r = self.es.search(index=index, body=body, _source_excludes=["embedding", "embedding_hint"])
        return [h["_source"] for h in r["hits"]["hits"]]

    def add_relation(self, from_urn: str, to_urn: str, relation: str):
        with self.driver.session() as s:
            s.run("MERGE (a {urn:$from}) MERGE (b {urn:$to}) MERGE (a)-[r:"+relation+"]->(b)",
                  {"from": from_urn, "to": to_urn})

    def get_recipe(self, urn: str):
        try:
            r = self.es.get(index="recipes", id=urn)
            return r["_source"]
        except Exception:
            return None
        
    def get_guide(self, urn: str):
        try:
            r = self.es.get(index="guides", id=urn, _source_excludes=["embedding"])
            return r["_source"]
        except Exception:
            return None

    def get_policy(self, urn: str):
        try:
            r = self.es.get(index="policies", id=urn, _source_excludes=["embedding"])
            return r["_source"]
        except Exception:
            return None

stores = Stores()