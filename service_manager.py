import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

import aiohttp


@dataclass
class ServiceConfig:
    port: str
    gpu: str
    health_check_url: Optional[str] = None
    last_health_check: Optional[datetime] = None
    is_healthy: bool = True
    current_load: int = 0
    max_load: int = 5  # Maximum concurrent requests per service

    def get_url(self, base_url: str = "http://localhost", endpoint: str = "") -> str:
        return f"{base_url}:{self.port}{endpoint}"


class ServiceStats:
    def __init__(self):
        self.total_requests = 0
        self.failed_requests = 0
        self.last_used = None
        self.last_error = None
        self.average_response_time = 0.0
        self.total_response_time = 0.0

    def update_stats(self, response_time: float, failed: bool = False):
        self.total_requests += 1
        self.last_used = datetime.now()
        if failed:
            self.failed_requests += 1
        else:
            self.total_response_time += response_time
            self.average_response_time = self.total_response_time / (
                    self.total_requests - self.failed_requests
            )


class ServiceBalancer:
    def __init__(self, services: List[Dict[str, str]], endpoint: str, max_connections: int = 10):
        self.configs = [ServiceConfig(**svc) for svc in services]
        self.endpoint = endpoint
        self.stats = {svc.port: ServiceStats() for svc in self.configs}
        self.lock = asyncio.Lock()
        self.session = None  # Initialized in async context

    async def initialize(self):
        if self.session is None:
            self.session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(limit=10, force_close=True)
            )

    async def cleanup(self):
        if self.session:
            await self.session.close()
            self.session = None

    async def get_next_service(self) -> ServiceConfig:
        async with self.lock:
            available_services = [svc for svc in self.configs if svc.is_healthy]
            if not available_services:
                raise RuntimeError("No healthy services available")

            selected_service = min(
                available_services,
                key=lambda s: (s.current_load, self.stats[s.port].average_response_time),
            )

            selected_service.current_load += 1
            self.stats[selected_service.port].last_used = datetime.now()
            return selected_service

    async def release_service(self, service: ServiceConfig):
        async with self.lock:
            service.current_load = max(0, service.current_load - 1)

    def get_stats(self) -> Dict:
        return {
            port: {
                "total_requests": stats.total_requests,
                "failed_requests": stats.failed_requests,
                "average_response_time": stats.average_response_time,
            }
            for port, stats in self.stats.items()
        }


class ServiceManager:
    def __init__(
            self,
            yolo_services: List[Dict[str, str]],
            caption_services: List[Dict[str, str]],
            rating_services: List[Dict[str, str]],
            max_workers: int = 4,
    ):
        self.yolo_balancer = ServiceBalancer(yolo_services, "/detect_batch_folder")
        self.caption_balancer = ServiceBalancer(caption_services, "/upload")
        self.rating_balancer = ServiceBalancer(rating_services, "/api")
        self.logger = logging.getLogger(__name__)

    async def initialize(self):
        await asyncio.gather(
            self.yolo_balancer.initialize(),
            self.caption_balancer.initialize(),
            self.rating_balancer.initialize(),
        )

    async def get_services(self, task_id: str) -> Dict[str, str]:
        try:
            self.logger.info(f"Fetching services for task {task_id}")

            yolo_service = await self.yolo_balancer.get_next_service()
            caption_service = await self.caption_balancer.get_next_service()
            rating_service = await self.rating_balancer.get_next_service()

            services = {
                "yolo_url": yolo_service.get_url(endpoint="/detect_batch_folder"),
                "caption_url": caption_service.get_url(endpoint="/upload"),
                "rating_url": rating_service.get_url(endpoint="/api"),
            }

            self.logger.info(f"Assigned services: {services}")
            return services

        except Exception as e:
            self.logger.error(f"Error fetching services: {e}")
            raise

    async def release_task_services(self, task_id: str, services: Dict[str, ServiceConfig]):
        for service_type, service in services.items():
            try:
                if service_type == "yolo_url":
                    await self.yolo_balancer.release_service(service)
                elif service_type == "caption_url":
                    await self.caption_balancer.release_service(service)
                elif service_type == "rating_url":
                    await self.rating_balancer.release_service(service)
            except Exception as e:
                self.logger.error(f"Error releasing service {service_type}: {e}")

    def get_stats(self) -> Dict:
        return {
            "yolo": self.yolo_balancer.get_stats(),
            "caption": self.caption_balancer.get_stats(),
            "rating": self.rating_balancer.get_stats(),
        }

    async def cleanup(self):
        await asyncio.gather(
            self.yolo_balancer.cleanup(),
            self.caption_balancer.cleanup(),
            self.rating_balancer.cleanup(),
        )