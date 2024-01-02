import random
import requests
import time
from datetime import timedelta, datetime

import vaderSentiment.vaderSentiment as vs
from alpaca.data import TimeFrame, Bar
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


def get_latest_price(ticker: str) -> float:
    bar: dict[str: Bar] = dataAPI.get_stock_latest_bar(StockLatestBarRequest(
        symbol_or_symbols=ticker
    ))
    return bar[ticker].close


def set_orders(targets: dict[str: float]):
    tradeAPI.cancel_orders()
    positions = {p.symbol: p for p in tradeAPI.get_all_positions()}

    change = False
    new_orders = {}
    for ticker, target in targets.items():
        current = int(positions[ticker].qty) if ticker in positions else 0
        price = float(positions[ticker].current_price) if ticker in positions else round(get_latest_price(ticker), 2)
        target = int(target/price)
        diff = current - target
        if not diff:
            continue
        
        qty = abs(diff)
        side = 'buy' if diff < 0 else 'sell'
        tradeAPI.submit_order(LimitOrderRequest(
            symbol=ticker,
            qty=qty,
            side=side,
            type='limit',
            limit_price=price,
            time_in_force='day'
        ))
        change = True
        new_orders[ticker] = {'qty': qty, 'side': side}

    if change:
        embeds = [
            {
                'title': t,
                'color': int(''.join([str(ord(c)) for c in t])) % (16**6),
                'description': f'{new_orders[t]['side']} {new_orders[t]['qty']}' if t in new_orders else '',
                'fields': [
                    {
                        'name': k,
                        'value': str(v),
                        'inline': True
                    }
                    for k, v in vars(pos).items()
                ]
            }
            for t, pos in positions.items() if t in new_orders
        ]
        requests.post(
            url=CFG.discord_webhook_url,
            json={
                'username': 'News Trader',
                'avatar_url': 'https://cdn-icons-png.flaticon.com/512/4177/4177587.png',
                'embeds': embeds
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
            print(f'Sleeping until {datetime.now() + timedelta(seconds=t)}')
            time.sleep(t)
        else:
            t = clock.next_open.timestamp() - datetime.now().timestamp() - 30
            print(f'Sleeping until {datetime.now() + timedelta(seconds=t)}')
            time.sleep(t)
            set_orders(get_investment_targets(TICKERS))
            time.sleep(30)


if __name__ == '__main__':
    main()
        