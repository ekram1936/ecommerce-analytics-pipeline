from pydantic_settings import BaseSettings
from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent

class KafkaSettings(BaseSettings):
    bootstrap_servers: str = "localhost:9092"
    orders_topic: str = "olist.orders"
    payments_topic: str = "olist.payments"
    items_topic: str = "olist.order_items"
    dead_letter_topic: str = "olist.dead_letter"
    consumer_group: str = "ecommerce-pipeline"

    class Config:
        env_prefix = "KAFKA_"
        env_file = ROOT_DIR / ".env"

class SparkSettings(BaseSettings):
    app_name: str = "EcommerceAnalyticsPipeline"
    master: str = "local[*]"
    delta_lake_path: str = str(ROOT_DIR / "delta-lake")
    checkpoint_path: str = str(ROOT_DIR / "delta-lake" / "checkpoints")

    class Config:
        env_prefix = "SPARK_"
        env_file = ROOT_DIR / ".env"

class SnowflakeSettings(BaseSettings):
    account: str = ""
    user: str = ""
    password: str = ""
    database: str = "ECOMMERCE_DB"
    warehouse: str = "COMPUTE_WH"
    schema_name: str = "ANALYTICS"
    role: str = "SYSADMIN"

    class Config:
        env_prefix = "SNOWFLAKE_"
        env_file = ROOT_DIR / ".env"

class DataSettings(BaseSettings):
    raw_data_path: str = str(ROOT_DIR / "data")
    replay_speed_factor: int = 1000  # 1000x faster than real time
    batch_size: int = 100

    class Config:
        env_prefix = "DATA_"
        env_file = ROOT_DIR / ".env"

class Settings(BaseSettings):
    kafka: KafkaSettings = KafkaSettings()
    spark: SparkSettings = SparkSettings()
    snowflake: SnowflakeSettings = SnowflakeSettings()
    data: DataSettings = DataSettings()
    environment: str = "development"
    log_level: str = "INFO"

    class Config:
        env_file = ROOT_DIR / ".env"

settings = Settings()
