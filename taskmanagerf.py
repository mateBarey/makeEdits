import asyncio
from uuid import uuid4

class TaskManager:
    def __init__(self):
        self.tasks = {}
        self.task_run_events = {}
        self.task_stop_events = {}

    async def controlled_coroutine(self, task_func, *args, **kwargs):
        task_uid = kwargs.pop('uid', None)
        if not task_uid:
            raise ValueError("UID is required for controlled coroutine")

        run_event = self.task_run_events.setdefault(task_uid, asyncio.Event())
        stop_event = self.task_stop_events.setdefault(task_uid, asyncio.Event())

        run_event.set()
        try:
            while not stop_event.is_set():
                await run_event.wait()
                if stop_event.is_set():
                    break
                await task_func(*args, **kwargs)
        except asyncio.CancelledError:
            print(f"Task UID: {task_uid} got cancelled")
        finally:
            if task_uid in self.tasks:
                del self.tasks[task_uid]

    def start_task(self, task_func, *args, **kwargs):
        task_uid = kwargs.get('uid')
        
        task = asyncio.create_task(self.controlled_coroutine(task_func, *args, **kwargs), name=str(task_uid))
        self.tasks[task_uid] = task
        return task_uid

    def pause_task(self, task_uid):
        if task_uid in self.task_run_events:
            print(f'task_uid: {task_uid}')
            print(f'task_run_event: {self.task_run_events[task_uid]}')
            self.task_run_events[task_uid].clear()

    def resume_task(self, task_uid):
        if task_uid in self.task_run_events:
            self.task_run_events[task_uid].set()

    def stop_task(self, task_uid):
        if task_uid in self.task_stop_events:
            print(f'task_uid: {task_uid}')
            print(f'task_stop_event: {self.task_stop_events[task_uid]}')
            self.task_stop_events[task_uid].set()
        if task_uid in self.tasks:
            self.tasks[task_uid].cancel()

    async def shutdown(self):
        for task_uid in list(self.tasks.keys()):
            self.stop_task(task_uid)
        await asyncio.gather(*self.tasks.values(), return_exceptions=True)