from pydantic_settings import BaseSettings, SettingsConfigDict


class MongoSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MONGODB_")

    uri: str = "mongodb://localhost:27017"
    database_name: str = "retry_scheduler"


class RedisSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="REDIS_")

    url: str = "redis://localhost:6379/0"
    queue_name: str = "jobs:queue"
