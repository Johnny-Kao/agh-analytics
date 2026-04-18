import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    AGH_BASE_URL: str = os.environ["AGH_BASE_URL"]
    AGH_USERNAME: str = os.environ["AGH_USERNAME"]
    AGH_PASSWORD: str = os.environ["AGH_PASSWORD"]

    PG_HOST: str = os.environ.get("PG_HOST", "localhost")
    PG_PORT: int = int(os.environ.get("PG_PORT", "5432"))
    PG_DB: str = os.environ.get("PG_DB", "agh_analytics")
    PG_USER: str = os.environ["PG_USER"]
    PG_PASSWORD: str = os.environ["PG_PASSWORD"]

    BATCH_SIZE: int = int(os.environ.get("BATCH_SIZE", "1000"))

    @property
    def pg_dsn(self) -> str:
        return (
            f"host={self.PG_HOST} port={self.PG_PORT} "
            f"dbname={self.PG_DB} user={self.PG_USER} "
            f"password={self.PG_PASSWORD}"
        )


config = Config()
