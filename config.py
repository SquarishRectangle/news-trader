from pydantic import BaseModel
import json, os

class _Config(BaseModel):
    alpaca_auth: dict
    newsapi_auth: dict
    discord_webhook_url: str
    news_api_key: str

s = {}
if os.path.exists('.secrets'):
    with open('.secrets', 'r') as f:
        s = json.load(f)
CFG = _Config(**s)
    