def recipe_index(dim: int):
    return {
        "mappings": {
            "properties": {
                "urn": {"type": "keyword"},
                "title": {"type": "text"},
                "description": {"type": "text"},
                "ingredients": {"type": "nested", "properties": {
                    "name": {"type": "text"},
                    "quantity": {"type": "keyword"},
                    "unit": {"type": "keyword"}
                }},
                "instructions": {"type": "text"},
                "tags": {"type": "keyword"},
                "nutrition": {"properties": {
                    "calories": {"type": "float"},
                    "protein": {"type": "float"},
                    "carbs": {"type": "float"},
                    "fat": {"type": "float"}
                }},
                "embedding": {
                    "type": "dense_vector",
                    "dims": dim,
                    "index": True,
                    "similarity": "cosine"
                }
            }
        }
    }

def guide_index(dim: int):
    return {
        "mappings": {
            "properties": {
                "urn": {"type": "keyword"},
                "title": {"type": "text"},
                "content": {"type": "text"},
                "topic": {"type": "keyword"},
                "tags": {"type": "keyword"},
                "embedding": {
                    "type": "dense_vector",
                    "dims": dim,
                    "index": True,
                    "similarity": "cosine"
                }
            }
        }
    }

def policy_index(dim: int):
    return {
        "mappings": {
            "properties": {
                "urn": {"type": "keyword"},
                "title": {"type": "text"},
                "content": {"type": "text"},
                "authority": {"type": "keyword"},
                "effective_date": {"type": "date"},
                "tags": {"type": "keyword"},
                "embedding": {
                    "type": "dense_vector",
                    "dims": dim,
                    "index": True,
                    "similarity": "cosine"
                }
            }
        }
    }