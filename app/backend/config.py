import os
from pydantic import BaseModel


class Settings(BaseModel):
    api_prefix: str = "/api"
    app_name: str = "Site Selection Platform"
    serving_endpoint: str = os.environ.get("SERVING_ENDPOINT", "YOUR_SERVING_ENDPOINT")
    chat_endpoint: str = os.environ.get("CHAT_ENDPOINT", "YOUR_CHAT_ENDPOINT")


conf = Settings()
