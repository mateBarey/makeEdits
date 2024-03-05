import asyncio
import logging

class AsyncCSVLogger:
    def __init__(self, filename: str, loop: asyncio.AbstractEventLoop):
        self.filename = filename
        self.loop = loop
        self.logger = logging.getLogger(self.filename)

    def setup_logger(self):
        handler = logging.FileHandler(self.filename, mode='w')
        formatter = logging.Formatter('%(asctime)s,%(levelname)s,%(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False

    async def log(self, level: int, message: str):
        await self.loop.run_in_executor(None, self.logger.log, level, message)

    async def close(self):
        for handler in self.logger.handlers:
            handler.close()
            self.logger.removeHandler(handler)

    async def __aenter__(self):
        self.setup_logger()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()