import random
import requests
import time
from datetime import timedelta, datetime

import vaderSentiment.vaderSentiment as vs
from alpaca.data import TimeFrame
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, StockLatestBarRequest
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import LimitOrderRequest
from rich import print as print
from tinydb import TinyDB, Query

from config import CFG
from web_ratelimit import WebLimiter

tradeAPI = TradingClient(**CFG.alpaca_auth)
dataAPI = StockHistoricalDataClient(**CFG.alpaca_auth)

sia = vs.SentimentIntensityAnalyzer()

DB = TinyDB('db.json')
sentiments = DB.table('sentiments')
Q = Query()

TICKERS = ['AAPL', 'AMZN', 'GOOG', 'MSFT', 'NVDA']
NAMES = {'AAPL': 'apple', 'AMZN': 'amazon', 'GOOG': 'google', 'MSFT': 'microsoft', 'NVDA': 'nvidia'}


def sentiment_function(input: float) -> float:
    return input**2 if input > 0 else 0


@WebLimiter.limit(1, 1)
def wget(url: str) -> requests.Response:
    return requests.get(url)


def get_ticker_sentiment(ticker: str) -> float:
    record = sentiments.search(Q.ticker == ticker)
    record = record[0] if record else False
    if not record or datetime.now() - datetime.fromisoformat(record['isotime']) > timedelta(minutes=12):
        res = wget(f'https://newsdata.io/api/1/news?apikey={CFG.news_api_key}&q={NAMES[ticker]}&timeframe=24&category=business&language=en&full_content=1')
        news = res.json()
        news_string = [f'{result['title']}\n{result['description']}\n{result['content']}\n\n' for result in news['results']]
        scores = sia.polarity_scores(news_string)
        if record:
            sentiments.update(
                {
                    'ticker': ticker,
                    'isotime': datetime.now().isoformat(),
                    'scores': scores
                },
                Q.ticker == ticker
            )
        else:
            sentiments.insert({
                'ticker': ticker,
                'isotime': datetime.now().isoformat(),
                'scores': scores
            })
        return scores['pos'] - scores['neg']
    return record['scores']['pos'] - record['scores']['neg']


def get_investment_targets(tickers: list[str]) -> dict[str: float]:
    worth = float(tradeAPI.get_account().equity)
    sentiments = {ticker: get_ticker_sentiment(ticker) for ticker in tickers}
    targets = {t: sentiment_function(sentiments[t]) for t in tickers}
    total = sum(targets.values())
    mult = worth/total if total > 0 else 0
    targets = {t: v * mult for t, v in targets.items()}
    return targets


def get_avg_prices(tickers: list[str]) -> dict[str: float]:
    bars = dataAPI.get_stock_bars(StockBarsRequest(
        symbol_or_symbols=tickers,
        timeframe=TimeFrame.Minute,
        start=datetime.now() - timedelta(hours=1)
    ))
    data = {t: sum(x.close for x in v)/len(v) for t, v in bars.data.items()}
    return data


def get_prices(tickers: list[str]) -> dict[str: float]:
    bar = dataAPI.get_stock_latest_bar(StockLatestBarRequest(
        symbol_or_symbols=tickers
    ))
    return {t: (v.high + v.low)/2 for t, v in bar.items()}


def set_orders(targets: dict[str: float]):
    tradeAPI.cancel_orders()
    positions = {p.symbol: p for p in tradeAPI.get_all_positions()}
    prices = get_prices(targets.keys())
    for ticker, target in targets.items():
        current = int(positions[ticker].qty) if ticker in positions else 0
        
        target = int(target/prices[ticker])
        diff = current - target
        if not diff:
            continue

        order = tradeAPI.submit_order(LimitOrderRequest(
            symbol=ticker,
            qty=abs(diff),
            side='buy' if diff < 0 else 'sell',
            type='limit',
            limit_price=round(prices[ticker], 2),
            time_in_force='day'
        ))

        requests.post(
            url=CFG.discord_webhook_url,
            json={
                'username': 'News Trader',
                'avatar_url': 'https://cdn-icons-png.flaticon.com/512/4177/4177587.png',
                'embeds': [
                    {
                        'title': 'New Order',
                        'description': f'{order.symbol}',
                        'color': 16750848 if order.side == 'sell' else 255,
                        'fields': [
                            {
                                'name': 'Quantity',
                                'value': order.qty,
                                'inline': True
                            },
                            {
                                'name': 'Limit Price',
                                'value': order.limit_price,
                                'inline': True
                            }
                        ]
                    }
                ]
            },
            headers={
                'Content-type': 'application/json'
            }
        )


def main():
    print('Starting')
    while True:
        clock = tradeAPI.get_clock()
        if clock.is_open:
            set_orders(get_investment_targets(TICKERS))
            t = random.randint(60 * 1, 60 * 2)
        else:
            t = clock.next_open.timestamp() - datetime.now().timestamp() + 10
        print(f'Sleeping until {datetime.now() + timedelta(seconds=t)}')
        time.sleep(t)


if __name__ == '__main__':
    main()
        