"""
Async Gamma API Client for Polymarket Markets

Converted from sync httpx to async aiohttp for better performance
and concurrent market fetching.
"""

import asyncio
import json
from typing import List, Dict, Optional

try:
    import aiohttp
    HAS_AIOHTTP = True
except ImportError:
    HAS_AIOHTTP = False
    print("Warning: aiohttp not installed. Install with: pip install aiohttp")

from agents.agents.utils.objects import Market, PolymarketEvent, ClobReward, Tag


class AsyncGammaMarketClient:
    """
    Async Gamma API client for Polymarket markets.
    
    Benefits over sync client:
    - Concurrent market fetching with asyncio.gather()
    - No thread context-switching overhead
    - Better for 50+ market monitoring
    """
    
    def __init__(self):
        self.gamma_url = "https://gamma-api.polymarket.com"
        self.gamma_markets_endpoint = self.gamma_url + "/markets"
        self.gamma_events_endpoint = self.gamma_url + "/events"
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Lazy initialization of aiohttp session."""
        if not HAS_AIOHTTP:
            raise ImportError("aiohttp is required. Install with: pip install aiohttp")
        
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session
    
    async def close(self):
        """Close the aiohttp session when done."""
        if self._session and not self._session.closed:
            await self._session.close()
    
    def parse_pydantic_market(self, market_object: dict) -> Market:
        """Parse market object into Pydantic Market model."""
        try:
            if "clobRewards" in market_object:
                clob_rewards: list[ClobReward] = []
                for clob_rewards_obj in market_object["clobRewards"]:
                    clob_rewards.append(ClobReward(**clob_rewards_obj))
                market_object["clobRewards"] = clob_rewards

            if "events" in market_object:
                events: list[PolymarketEvent] = []
                for market_event_obj in market_object["events"]:
                    events.append(self.parse_nested_event(market_event_obj))
                market_object["events"] = events

            # These two fields are returned as stringified lists from API
            if "outcomePrices" in market_object:
                market_object["outcomePrices"] = json.loads(
                    market_object["outcomePrices"]
                )
            if "clobTokenIds" in market_object:
                market_object["clobTokenIds"] = json.loads(
                    market_object["clobTokenIds"]
                )

            return Market(**market_object)
        except Exception as err:
            print(f"[parse_market] Caught exception: {err}")
            print("exception while handling object:", market_object)
            return None
    
    def parse_nested_event(self, event_object: dict) -> PolymarketEvent:
        """Parse nested event object into Pydantic model."""
        try:
            if "tags" in event_object:
                tags: list[Tag] = []
                for tag in event_object["tags"]:
                    tags.append(Tag(**tag))
                event_object["tags"] = tags
            return PolymarketEvent(**event_object)
        except Exception as err:
            print(f"[parse_event] Caught exception: {err}")
            print("\n", event_object)
            return None
    
    def parse_pydantic_event(self, event_object: dict) -> PolymarketEvent:
        """Parse event object into Pydantic model."""
        try:
            if "tags" in event_object:
                tags: list[Tag] = []
                for tag in event_object["tags"]:
                    tags.append(Tag(**tag))
                event_object["tags"] = tags
            return PolymarketEvent(**event_object)
        except Exception as err:
            print(f"[parse_event] Caught exception: {err}")
            return None
    
    async def get_markets(
        self, 
        querystring_params: Optional[Dict] = None,
        parse_pydantic=False,
        local_file_path=None
    ) -> List[Dict] | List[Market]:
        """
        Fetch markets from Gamma API asynchronously.
        
        Args:
            querystring_params: Query parameters for the API
            parse_pydantic: Return Pydantic Market objects instead of raw dicts
            local_file_path: Save response to file (for testing)
        
        Returns:
            List of Market objects or raw dicts
        """
        if querystring_params is None:
            querystring_params = {}
        
        if parse_pydantic and local_file_path is not None:
            raise Exception(
                'Cannot use "parse_pydantic" and "local_file" params simultaneously.'
            )

        session = await self._get_session()
        
        async with session.get(self.gamma_markets_endpoint, params=querystring_params) as resp:
            if resp.status == 200:
                data = await resp.json()
                
                if local_file_path is not None:
                    with open(local_file_path, "w+") as out_file:
                        json.dump(data, out_file)
                elif not parse_pydantic:
                    return data
                else:
                    markets: list[Market] = []
                    for market_object in data:
                        parsed = self.parse_pydantic_market(market_object)
                        if parsed:
                            markets.append(parsed)
                    return markets
            else:
                print(f"Error response from API: HTTP {resp.status}")
                raise Exception(f"Gamma API error: HTTP {resp.status}")
    
    async def get_events(
        self,
        querystring_params: Optional[Dict] = None,
        parse_pydantic=False,
        local_file_path=None
    ) -> List[Dict] | List[PolymarketEvent]:
        """
        Fetch events from Gamma API asynchronously.
        
        Args:
            querystring_params: Query parameters for the API
            parse_pydantic: Return Pydantic PolymarketEvent objects
            local_file_path: Save response to file
        
        Returns:
            List of PolymarketEvent objects or raw dicts
        """
        if querystring_params is None:
            querystring_params = {}
        
        if parse_pydantic and local_file_path is not None:
            raise Exception(
                'Cannot use "parse_pydantic" and "local_file" params simultaneously.'
            )

        session = await self._get_session()
        
        async with session.get(self.gamma_events_endpoint, params=querystring_params) as resp:
            if resp.status == 200:
                data = await resp.json()
                
                if local_file_path is not None:
                    with open(local_file_path, "w+") as out_file:
                        json.dump(data, out_file)
                elif not parse_pydantic:
                    return data
                else:
                    events: list[PolymarketEvent] = []
                    for market_event_obj in data:
                        parsed = self.parse_pydantic_event(market_event_obj)
                        if parsed:
                            events.append(parsed)
                    return events
            else:
                raise Exception(f"Gamma API error: HTTP {resp.status}")
    
    async def get_all_markets(self, limit: int = 2) -> List[Dict]:
        """Fetch markets with limit."""
        return await self.get_markets(querystring_params={"limit": limit})
    
    async def get_all_events(self, limit: int = 2) -> List[Dict]:
        """Fetch events with limit."""
        return await self.get_events(querystring_params={"limit": limit})
    
    async def get_current_markets(self, limit: int = 4) -> List[Dict]:
        """Fetch currently active markets."""
        return await self.get_markets(
            querystring_params={
                "active": True,
                "closed": False,
                "archived": False,
                "limit": limit,
            }
        )
    
    async def get_all_current_markets(
        self, 
        limit: int = 100, 
        max_pages: int = 10
    ) -> List[Dict]:
        """
        Fetch all active markets concurrently.
        
        Args:
            limit: Markets per page
            max_pages: Maximum pages to fetch (safety limit)
        
        Returns:
            List of all active markets
        
        Performance: Fetches all pages concurrently using asyncio.gather()
        instead of sequentially, reducing total time dramatically.
        """
        tasks = []
        
        # Fetch first page to determine total
        first_page = await self.get_markets(
            querystring_params={
                "active": True,
                "closed": False,
                "archived": False,
                "limit": limit,
                "offset": 0,
            }
        )
        
        if len(first_page) == 0:
            return first_page
        
        all_markets = first_page
        
        # If more markets exist, fetch remaining pages concurrently
        if len(first_page) == limit:
            # Estimate total pages (cap at max_pages for safety)
            num_pages = min(max_pages, 10)  # Cap at 10 pages for now
            
            for page in range(1, num_pages):
                offset = page * limit
                task = self.get_markets(
                    querystring_params={
                        "active": True,
                        "closed": False,
                        "archived": False,
                        "limit": limit,
                        "offset": offset,
                    }
                )
                tasks.append(task)
            
            # Fetch all pages concurrently
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for result in results:
                    if isinstance(result, Exception):
                        print(f"Error fetching market page: {result}")
                    elif result:
                        all_markets.extend(result)
                        
                        # Stop if we got fewer results than limit
                        if len(result) < limit:
                            break
        
        print(f"Fetched {len(all_markets)} total markets")
        return all_markets
    
    async def get_current_events(self, limit: int = 4) -> List[Dict]:
        """Fetch currently active events."""
        return await self.get_events(
            querystring_params={
                "active": True,
                "closed": False,
                "archived": False,
                "limit": limit,
            }
        )
    
    async def get_clob_tradable_markets(self, limit: int = 2) -> List[Dict]:
        """Fetch markets with enabled order books."""
        return await self.get_markets(
            querystring_params={
                "active": True,
                "closed": False,
                "archived": False,
                "limit": limit,
                "enableOrderBook": True,
            }
        )
    
    async def get_market(self, market_id: str) -> Dict:
        """Fetch a single market by ID."""
        url = f"{self.gamma_markets_endpoint}/{market_id}"
        session = await self._get_session()
        
        async with session.get(url) as resp:
            return await resp.json()


# Convenience alias for backward compatibility
GammaMarketClientAsync = AsyncGammaMarketClient


if __name__ == "__main__":
    async def main():
        """Example usage of async Gamma client."""
        gamma = AsyncGammaMarketClient()
        
        try:
            # Fetch a single market
            market = await gamma.get_market("253123")
            print(f"Market: {market}")
            
            # Fetch current markets concurrently
            markets = await gamma.get_current_markets(limit=10)
            print(f"Fetched {len(markets)} markets")
            
            # Fetch all active markets (concurrent)
            all_markets = await gamma.get_all_current_markets(limit=100)
            print(f"Total active markets: {len(all_markets)}")
            
        finally:
            await gamma.close()
    
    asyncio.run(main())