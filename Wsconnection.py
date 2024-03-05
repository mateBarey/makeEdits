import time
from typing import Optional
import asyncio
from asyncio import *
import json 
import aiohttp
from aiohttp import ClientSession, ClientWebSocketResponse, WSMsgType
from enum import Enum, auto

ws_url = 'wss://api.dydx.exchange/v3/ws'

class ConnectionState(Enum):
    DISCONNECTED = auto()
    CONNECTING = auto()
    CONNECTED = auto()
    DISCONNECTING = auto()

class WSConnection:
    def __init__(self, market: str = None, channel: float = None,safety_factor: float = 0.05, ping_interval: float = 1.5):
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._state = ConnectionState.DISCONNECTED
        self._market = market
        self._channel = channel  
        self._safety_factor = safety_factor
        self._ping_interval = ping_interval
        self._disconnect_after_response = False 

    @property
    def connection_state(self) -> ConnectionState:
        return self._state
    
    async def connect(self, ws_url: str):
        if self._state == ConnectionState.CONNECTED:
            return  # Already connected
        self._state = ConnectionState.CONNECTING
        self._session = ClientSession()
        self._ws = await self._session.ws_connect(ws_url)
        self._state = ConnectionState.CONNECTED
        asyncio.create_task(self._send_pings())

    async def disconnect(self):
        if self._state in [ConnectionState.DISCONNECTED, ConnectionState.DISCONNECTING]:
            return  # Already disconnected or disconnecting
        self._state = ConnectionState.DISCONNECTING
        if self._ws is not None:
            await self._ws.close()
        if self._session is not None:
            await self._session.close()
        self._state = ConnectionState.DISCONNECTED
     

    async def send_json(self, data: dict):
        message = json.dumps(data)
        await asyncio.sleep(0.5*(1 + self._safety_factor))
        self._ensure_connected()
        await self._ws.send_str(message)
    
    async def receive_json(self) -> Optional[dict]:
        self._ensure_connected()
        msg = await self._ws.receive()

        if msg.type == aiohttp.WSMsgType.TEXT:
            return json.loads(msg.data)

        elif msg.type == aiohttp.WSMsgType.CLOSED:
            self._state = ConnectionState.DISCONNECTED
            return None
    
    async def initiate_disconnect(self):
        # Logic to initiate the disconnection process
        if self._state == ConnectionState.CONNECTED:
            await asyncio.sleep(1)
            await self.disconnect()
    
    async def _send_pings(self):
        while self._state == ConnectionState.CONNECTED:
            await asyncio.sleep(self._ping_interval)
            if self._state == ConnectionState.CONNECTED:
                await self._ws.ping()

    def _ensure_not_connected(self):
        if self._state != ConnectionState.DISCONNECTED:
            raise RuntimeError("WebSocket is already connected or connecting.")

    def _ensure_connected(self):
        if self._state != ConnectionState.CONNECTED:
            raise RuntimeError("WebSocket is not connected.")

    def _update_last_recv_time(self):
        self._last_recv_time = time.time()


