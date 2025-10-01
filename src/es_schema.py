def recipe_collection_index(dim: int):
    return {
        "mappings": {
            "properties": {
                "urn": {"type": "keyword"},
                "title": {"type": "text"},
                "description": {"type": "text"},
                "ingredients": {
                    "type": "nested",
                    "properties": {
                        "name": {"type": "text"},
                        "quantity": {"type": "text"},
                    },
                },
                "instructions": {"type": "text"},
                "tags": {"type": "keyword"},
            }
        }
    }


def artifact_index(dim: int):
    """
    Elasticsearch mapping for artifacts. 
    """
    return {
        "mappings": {
            "properties": {
                "id": {"type": "keyword"}, 
                "parent_urn": {"type": "keyword"}, 
                "type": {"type": "keyword"},
                # Core metadata
                "title": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "description": {
                    "type": "text"
                },
                "creator": {"type": "keyword"},  
                "language": {"type": "keyword"},
                # File info
                "file_url": {"type": "keyword"},
                "file_s3_url": {"type": "keyword"},
                "file_type": {"type": "keyword"},
                "file_size": {"type": "long"},
                "created_at": {
                    "type": "date",
                    "format": "strict_date_optional_time||epoch_millis",
                },
                "updated_at": {
                    "type": "date",
                    "format": "strict_date_optional_time||epoch_millis",
                },
            }
        }
    }


def guide_index(dim: int):
    """
    Elasticsearch index mapping for GuideSchema.
    Aligns with GuideCreationSchema and GuideSchema fields.
    """
    return {
        "mappings": {
            "properties": {
                # System-generated fields
                "urn": {"type": "keyword"},
                "id": {"type": "keyword"},
                "creator": {"type": "keyword"},
                "created_at": {"type": "date"},
                "updated_at": {"type": "date"},
                "publication_date": {"type": "date"},
                # User-provided metadata
                "title": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "description": {"type": "text"},
                "tags": {"type": "keyword"},
                "status": {"type": "keyword"},
                "url": {"type": "keyword"},
                "license": {"type": "keyword"},
                "region": {"type": "keyword"},
                "language": {"type": "keyword"},
                # Guide-specific fields
                "content": {"type": "text"},
                "topic": {"type": "keyword"},
                "audience": {"type": "keyword"},
                "type": {"type": "keyword"},
                # Nested artifacts
                "artifacts": {
                    "type": "nested",
                    "properties": {
                        "urn": {"type": "keyword"},
                        "id": {"type": "keyword"},
                        "title": {"type": "text"},
                        "description": {"type": "text"},
                        "file_url": {"type": "keyword"},
                        "file_type": {"type": "keyword"},
                        "file_size": {"type": "long"},
                        "created_at": {"type": "date"},
                        "updated_at": {"type": "date"},
                        "type": {"type": "keyword"},
                    },
                },
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
            }
        }
    }


def paper_index(dim: int):
    return {
        "mappings": {
            "properties": {
                "urn": {"type": "keyword"},
                "title": {"type": "text"},
                "abstract": {"type": "text"},
                "authors": {"type": "keyword"},
                "publication_date": {"type": "date"},
                "journal": {"type": "keyword"},
                "tags": {"type": "keyword"},
            }
        }
    }


def foodtable_index(dim: int):
    return {
        "mappings": {
            "properties": {
                "urn": {"type": "keyword"},
                "name": {"type": "text"},
                "category": {"type": "keyword"},
                "nutrients": {
                    "properties": {
                        "calories": {"type": "float"},
                        "protein": {"type": "float"},
                        "carbs": {"type": "float"},
                        "fat": {"type": "float"},
                    }
                },
                "serving_size": {"type": "keyword"},
                "tags": {"type": "keyword"},
            }
        }
    }


def organization_index(dim: int):
    return {
        "mappings": {
            "properties": {
                "urn": {"type": "keyword"},
                "name": {"type": "text"},
                "description": {"type": "text"},
                "industry": {"type": "keyword"},
                "image_url": {"type": "keyword"},
                "location": {"type": "keyword"},
                "tags": {"type": "keyword"},
            }
        }
    }


def person_index(dim: int):
    return {
        "mappings": {
            "properties": {
                "urn": {"type": "keyword"},
                "name": {"type": "text"},
                "bio": {"type": "text"},
                "role": {"type": "keyword"},
                "organization": {"type": "keyword"},
                "image_url": {"type": "keyword"},
                "tags": {"type": "keyword"},
            }
        }
    }
