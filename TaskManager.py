
import asyncio 

class TaskManager:
    def __init__(self):
        self.events = {}

    async def controlled_coroutine(self, uid, task_coro):
        event = self.events.get(uid, asyncio.Event())
        try:
            await event.wait()  # Wait for the event to be set
            await task_coro  # Execute the task coroutine
        except asyncio.CancelledError:
            # Handle the coroutine shutdown process
            print(f"Shutting down controlled task for {uid}")
            # Perform any cleanup or shutdown operations here
            pass

    def set_event(self, uid):
        if uid in self.events:
            self.events[uid].set()  # Trigger the event

    def create_controlled_task(self, uid, task_coro):
        if uid not in self.events:
            self.events[uid] = asyncio.Event()
        return asyncio.create_task(self.controlled_coroutine(uid, task_coro))

    def shutdown(self):
        for event in self.events.values():
            event.set()  # Trigger all events for shutdown
