import asyncio
from collections import defaultdict
import time 

class BatchRequestProcessor:
    def __init__(self, safety_factor=0.05, window_sec=10):
        self.safety_factor = safety_factor
        self.window_sec = window_sec
        self.batched_requests = []
        self.response_objects = {}
        self.remaining_rate_limits = defaultdict(int)
        self.locks = {}
        self.rate_limit_windows = {}
        self.retry_windows = {}

    async def f_execute(self, request, path, uid):
        response = await request()
        self.response_objects[uid] = response
        self.update_rate_limit(path, response)
        await self.handle_retry(response, path)

    def update_rate_limit(self, path, response):
        rate_limit_remaining = int(response.headers.get('RateLimit-Remaining', 0))
        adjusted_rate_limit = rate_limit_remaining * (1 - self.safety_factor)
        self.remaining_rate_limits[path] = adjusted_rate_limit

    async def handle_retry(self, response, path):
        if 'Retry-After' in response.headers:
            retry_after = int(response.headers['Retry-After']) / 1000
            adjusted_retry_after = retry_after * (1 + self.safety_factor)
            self.retry_windows[path] = time.time() + adjusted_retry_after
            await asyncio.sleep(adjusted_retry_after)

    async def execute(self):
        while self.batched_requests:
            await self.remove_completed_tasks()
            prioritized_requests, standard_requests = self.group_requests_by_priority()
            await self.process_request_groups(prioritized_requests)
            await self.process_request_groups(standard_requests)

    async def process_request_groups(self, grouped_requests):
        for path, requests in self.group_requests_by_path(grouped_requests).items():
            await self.execute_requests_for_path(path, requests)

    async def execute_requests_for_path(self, path, requests):
        lock = self.get_or_create_lock(path)
        async with lock:
            for request_info in requests:
                await self.process_request(path, request_info)

    async def process_request(self, path, request_info):
        uid, request = request_info['uid'], request_info['request']
        remaining_rate_limit = self.remaining_rate_limits.get(path, float('inf'))
        if remaining_rate_limit > 0:
            await self.f_execute(request, path, uid)

    def group_requests_by_priority(self):
        prioritized_requests = []
        standard_requests = []
        for request_info in self.batched_requests:
            if request_info.get('priority', False):
                prioritized_requests.append(request_info)
            else:
                standard_requests.append(request_info)
        return prioritized_requests, standard_requests

    def group_requests_by_path(self, requests):
        grouped_requests = defaultdict(list)
        for request_info in requests:
            path = request_info['path']
            grouped_requests[path].append(request_info)
        return grouped_requests

    async def add_request(self, uid, path, request_fn, rate_limit, redn, priority):
        self.batched_requests.append({'uid': uid, 'path': path, 'request': request_fn, 'rate_limit': rate_limit, 'redn': redn, 'priority': priority})

    async def remove_completed_tasks(self):
        self.batched_requests = [r for r in self.batched_requests if r['uid'] not in self.response_objects]

    async def get_response(self, uid):
        if uid in self.response_objects:
            return self.response_objects[uid]
        return None

    def get_or_create_lock(self, path):
        if path not in self.locks:
            self.locks[path] = asyncio.Lock()
        return self.locks[path]