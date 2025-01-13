# service_manager.py
import asyncio
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional
import aiohttp
from asyncio import Lock
import psutil


# Configuration class for service endpoints
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


# Class to track service statistics
class ServiceStats:
    def __init__(self):
        self.total_requests = 0
        self.failed_requests = 0
        self.last_used = None
        self.last_error = None
        self.average_response_time = 0.0
        self.total_response_time = 0.0
        self.concurrent_requests = 0

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


# Load balancer for specific service type
class ServiceBalancer:
    def __init__(self, services: List[Dict[str, str]], endpoint: str, max_connections: int = 10):
        self.configs = [ServiceConfig(**svc) for svc in services]
        self.endpoint = endpoint
        self.max_connections = max_connections
        self.stats = {svc.port: ServiceStats() for svc in self.configs}
        self.lock = Lock()
        self.logger = logging.getLogger(__name__)
        self.session = None  # Initialize in async context

    async def initialize(self):
        """Initialize session asynchronously"""
        if self.session is None:
            self.session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(
                    limit=self.max_connections,
                    force_close=True
                )
            )

    async def cleanup(self):
        """Cleanup session properly"""
        if self.session:
            await self.session.close()
            self.session = None

    async def get_next_service(self) -> ServiceConfig:
        """Get next available service with load balancing"""
        async with self.lock:
            available_services = [svc for svc in self.configs if svc.is_healthy]
            if not available_services:
                raise RuntimeError("No healthy services available")

            # Select service with lowest load
            selected_service = min(
                available_services,
                key=lambda s: (s.current_load / s.max_load, self.stats[s.port].total_requests)
            )

            selected_service.current_load += 1
            self.stats[selected_service.port].concurrent_requests += 1

            return selected_service

    async def release_service(self, service: ServiceConfig):
        """Release service after use"""
        async with self.lock:
            service.current_load = max(0, service.current_load - 1)
            self.stats[service.port].concurrent_requests = max(
                0,
                self.stats[service.port].concurrent_requests - 1
            )

    def get_stats(self) -> Dict:
        """Get service statistics"""
        return {
            port: {
                "total_requests": stats.total_requests,
                "failed_requests": stats.failed_requests,
                "concurrent_requests": stats.concurrent_requests,
                "average_response_time": stats.average_response_time
            }
            for port, stats in self.stats.items()
        }


# Main service manager class
class ServiceManager:
    def __init__(
            self,
            yolo_services: List[Dict[str, str]],
            caption_services: List[Dict[str, str]],
            rating_services: List[Dict[str, str]],
            max_workers: int = 4
    ):
        # Initialize service balancers
        self.max_connections_per_worker = 100 // max_workers
        self.yolo_balancer = ServiceBalancer(
            yolo_services,
            "/detect_batch_folder",
            max_connections=self.max_connections_per_worker
        )
        self.caption_balancer = ServiceBalancer(
            caption_services,
            "/upload",
            max_connections=self.max_connections_per_worker
        )
        self.rating_balancer = ServiceBalancer(
            rating_services,
            "/api",
            max_connections=self.max_connections_per_worker
        )

        self.logger = logging.getLogger(__name__)
        self.worker_id = os.getpid()
        self.active_services = {}

    async def get_services(self, task_id: str) -> Dict[str, str]:
        """Get service URLs for a task"""
        try:
            self.logger.info(f"Worker {self.worker_id} - Getting services for task {task_id}")

            # Get services using load balancing
            yolo_service = await self.yolo_balancer.get_next_service()
            caption_service = await self.caption_balancer.get_next_service()
            rating_service = await self.rating_balancer.get_next_service()

            services = {
                "yolo_url": yolo_service.get_url(endpoint="/detect_batch_folder"),
                "caption_url": caption_service.get_url(endpoint="/upload"),
                "rating_url": rating_service.get_url(endpoint="/api")
            }

            # Store active services for cleanup
            self.active_services[task_id] = {
                "yolo": yolo_service,
                "caption": caption_service,
                "rating": rating_service
            }

            return services

        except Exception as e:
            self.logger.error(f"Error getting services: {str(e)}")
            raise

    async def release_task_services(self, task_id: str):
        """Release services for a task"""
        if task_id in self.active_services:
            services = self.active_services[task_id]
            try:
                await self.yolo_balancer.release_service(services["yolo"])
                await self.caption_balancer.release_service(services["caption"])
                await self.rating_balancer.release_service(services["rating"])
            finally:
                del self.active_services[task_id]

    def get_stats(self) -> Dict:
        """Get statistics for all services"""
        return {
            "yolo": self.yolo_balancer.get_stats(),
            "caption": self.caption_balancer.get_stats(),
            "rating": self.rating_balancer.get_stats(),
            "worker_id": self.worker_id,
            "memory_usage": psutil.Process(self.worker_id).memory_info().rss / 1024 / 1024  # MB
        }

    async def cleanup(self):
        """Cleanup all resources"""
        await self.yolo_balancer.cleanup()
        await self.caption_balancer.cleanup()
        await self.rating_balancer.cleanup()