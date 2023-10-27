from pydantic import BaseModel
from typing import List


class WebServerRequest(BaseModel):
    youtube_id: str
    user_id: str
    ydx_app_host: str
    ydx_server: str
    AI_USER_ID: str
    model_config = {
        "json_schema_extra": {
            "examples": {
                "youtube_id": "dQw4w9WgXcQ",
                "user_id": "user1",
                "ydx_app_host": "http://localhost:8080",
                "ydx_server": "http://localhost:5000",
                "AI_USER_ID": "user1",
            }
        }
    }
    