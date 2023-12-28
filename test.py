import asyncio
import yaml
from datetime import timedelta, datetime

import requests
import httpx
from httpx import AsyncClient
import vaderSentiment.vaderSentiment as vs

from newsapi import NewsApiClient
from readabilipy import simple_json_from_html_string
from tinydb import TinyDB, Query

from rich import print as print
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio

from config import CFG
from web_ratelimit import AsyncWebLimiter, WebLimiter

@WebLimiter.limit(1, 1)
def wget(url: str) -> requests.Response:
    return requests.get(url)

@AsyncWebLimiter.limit(1)
async def async_wget(url: str) -> httpx.Response:
    async with AsyncClient() as client: 
        return await client.get(url) 

sia = vs.SentimentIntensityAnalyzer()
newsAPI = NewsApiClient(**CFG.newsapi_auth)

DB = TinyDB('db.json')
sentiments = DB.table('sentiments')
Q = Query()

NAMES = {'AAPL': 'apple', 'MSFT': 'microsoft', 'GOOG': 'google', 'AMZN': 'amazon', 'NVDA': 'nvidia'}
NEWS_KEY = 'pub_355417d0e9ffadc45ba6e20c77c7d1c1fc166'

def get_ticker_sentiment(ticker: str) -> float:
    record = sentiments.search(Q.ticker == ticker)
    record = record[0] if record else False
    if not record or datetime.fromisoformat(record['isotime']) - datetime.now() > timedelta(minutes=12):
        res = wget(f'https://newsdata.io/api/1/news?apikey={NEWS_KEY}&q={NAMES[ticker]}&timeframe=24&category=business&language=en&full_content=1')
        news = res.json()
        news_string = [f'{result['title']}\n{result['description']}\n{result['content']}\n\n' for result in news['results']]
        scores = sia.polarity_scores(news_string)
        sentiments.insert({
            'ticker': ticker,
            'isotime': datetime.now().isoformat(),
            'scores': scores
        })
        return scores['compound']
    return record['scores']['compound']

print(get_ticker_sentiment('AAPL'))
        

