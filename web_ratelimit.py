import asyncio

from typing import Callable
from ratelimit import sleep_and_retry, limits
from contextlib import contextmanager

class WebLimiter:
    _sites: dict[str: Callable] = {}

    @staticmethod
    def limit(calls=15, period=900):
        def wrapper(func: Callable):
            def inner(url: str):
                tld = url.split('/')[2]
                
                if not tld in WebLimiter._sites:
                    @sleep_and_retry
                    @limits(calls, period)
                    def temp(url: str):
                        return func(url)
                    WebLimiter._sites[tld] = temp
                
                return WebLimiter._sites[tld](url)
            return inner
        return wrapper
    
class AsyncWebLimiter:
    _sites: dict[str: asyncio.Semaphore] = {}

    @staticmethod
    def limit(delay: int = 1, connections: int = 1):
        def wrapper(func: Callable):
            async def inner(url: str):
                tld = url.split('/')[2]
                
                if not tld in WebLimiter._sites:
                    AsyncWebLimiter._sites[tld] = asyncio.Semaphore(connections)
                
                
                @contextmanager
                async def ctx():
                    async with AsyncWebLimiter._sites[tld]:
                        yield await func(url)
                        asyncio.sleep(delay)
                        
                async with ctx as res:
                    return res
            
            return inner
        return wrapper