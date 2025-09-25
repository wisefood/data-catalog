from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    ES_HOST: str = "http://elasticsearch:9200"
    ES_INDEX: str = "recipes"
    ES_DIM: int = 384

    NEO4J_URI: str = "bolt://neo4j:7687"      
    NEO4J_USER: str = "neo4j"
    NEO4J_PASSWORD: str = "neo4jpassword"

    API_PORT: int = 8080
    class Config:
        env_file = ".env"

settings = Settings()
