from pydantic import BaseModel


class Settings(BaseModel):
    api_prefix: str = "/api"
    app_name: str = "Site Selection Platform"


conf = Settings()
