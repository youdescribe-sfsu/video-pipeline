import asyncio
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Any
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
        self.active_services = set()
        self.configs = [ServiceConfig(**svc) for svc in services]
        self.endpoint = endpoint
        self.max_connections = max_connections
        self.stats = {svc.port: ServiceStats() for svc in self.configs}
        self.lock = Lock()  # Now using asyncio.Lock
        self.logger = logging.getLogger(__name__)
        self.session = None

    async def check_services_health(self) -> Dict[str, Any]:
        """
        Check health of all services using appropriate methods for each service type.
        Returns detailed health status information.
        """
        health_results = {}

        for service in self.configs:
            try:
                # Build the base URL without the endpoint
                base_url = service.get_url().rsplit(':', 1)[0] + ':' + service.port
                is_healthy = False
                error_message = None

                # Different health check strategies based on endpoint type
                if self.endpoint == "/detect_batch_folder":  # YOLO service
                    # YOLO services respond with 405 (Method Not Allowed) for GET on POST endpoints
                    async with self.session.get(
                            f"{base_url}/detect_batch_folder",
                            timeout=5
                    ) as response:
                        is_healthy = response.status in (405, 404, 200)

                elif self.endpoint == "/upload":  # Caption service
                    # Caption services usually return 404 for GET requests
                    async with self.session.get(
                            f"{base_url}/",
                            timeout=5
                    ) as response:
                        is_healthy = response.status in (404, 200)

                elif self.endpoint == "/api":  # Rating service
                    # Rating services typically have a root endpoint
                    async with self.session.get(
                            f"{base_url}/",
                            timeout=5
                    ) as response:
                        is_healthy = response.status in (200, 404)

                # Log detailed information about the health check
                self.logger.info(
                    f"Health check for {self.endpoint} service on port {service.port}: "
                    f"Status={response.status}, Healthy={is_healthy}"
                )

                # Store comprehensive health information
                health_results[service.port] = {
                    'healthy': is_healthy,
                    'gpu': service.gpu,
                    'current_load': service.current_load,
                    'last_check': datetime.now().isoformat(),
                    'endpoint_type': self.endpoint,
                    'response_status': response.status,
                    'error': error_message
                }

                # Update service status
                service.is_healthy = is_healthy
                service.last_health_check = datetime.now()

            except asyncio.TimeoutError as e:
                self.logger.warning(
                    f"Timeout while checking {self.endpoint} service on port {service.port}"
                )
                health_results[service.port] = self._create_error_result(
                    service, "Timeout during health check"
                )

            except aiohttp.ClientError as e:
                self.logger.error(
                    f"Connection error for {self.endpoint} service on port {service.port}: {str(e)}"
                )
                health_results[service.port] = self._create_error_result(
                    service, f"Connection error: {str(e)}"
                )

            except Exception as e:
                self.logger.error(
                    f"Unexpected error checking {self.endpoint} service on port {service.port}: {str(e)}"
                )
                health_results[service.port] = self._create_error_result(
                    service, f"Unexpected error: {str(e)}"
                )

        return health_results

    def _create_error_result(self, service: ServiceConfig, error_message: str) -> Dict:
        """Helper method to create consistent error results"""
        return {
            'healthy': False,
            'gpu': service.gpu,
            'current_load': service.current_load,
            'last_check': datetime.now().isoformat(),
            'endpoint_type': self.endpoint,
            'error': error_message
        }

    async def release_all(self):
        """Release all services in this balancer"""
        async with self.lock:
            for service in self.configs:
                service.current_load = 0
            self.active_services.clear()

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
        """Service selection with async lock"""
        async with self.lock:  # Use async context manager
            available_services = [svc for svc in self.configs if svc.is_healthy]
            if not available_services:
                raise RuntimeError("No healthy services available")

            selected_service = min(
                available_services,
                key=lambda s: s.current_load
            )
            selected_service.current_load += 1
            return selected_service

    async def release_service(self, service: ServiceConfig):
        """Release service with async lock"""
        async with self.lock:
            service.current_load = max(0, service.current_load - 1)

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
        self._initialized = False

    async def initialize(self):
        """Initialize with more lenient health checking"""
        if self._initialized:
            return

        try:
            # Initialize all balancers
            await self.yolo_balancer.initialize()
            await self.caption_balancer.initialize()
            await self.rating_balancer.initialize()

            # Check if at least one service of each type is healthy
            health_status = await self.check_all_services_health()

            has_healthy_yolo = any(
                status['healthy']
                for status in health_status['yolo_services'].values()
            )
            has_healthy_caption = any(
                status['healthy']
                for status in health_status['caption_services'].values()
            )
            has_healthy_rating = any(
                status['healthy']
                for status in health_status['rating_services'].values()
            )

            if not (has_healthy_yolo and has_healthy_caption and has_healthy_rating):
                raise RuntimeError(
                    "Service initialization failed: "
                    "At least one service of each type must be healthy"
                )

            self._initialized = True
            self.logger.info("Service manager initialized successfully")

        except Exception as e:
            self.logger.error(f"Service manager initialization failed: {str(e)}")
            await self.cleanup()
            raise

    async def check_all_services_health(self) -> Dict[str, Any]:
        """
        Checks the health of all registered services with improved error handling.
        """
        try:
            health_status = {
                'caption_services': await self.caption_balancer.check_services_health(),
                'rating_services': await self.rating_balancer.check_services_health(),
                'yolo_services': await self.yolo_balancer.check_services_health()
            }

            # Add overall health status
            all_services_healthy = all(
                all(service.get('healthy', False)
                    for service in service_type.values())
                for service_type in health_status.values()
            )

            health_status['overall_health'] = {
                'healthy': all_services_healthy,
                'timestamp': datetime.now().isoformat()
            }

            return health_status
        except Exception as e:
            self.logger.error(f"Error checking services health: {str(e)}")
            raise

    async def ensure_initialized(self):
        """
        Ensures all service balancers are properly initialized.
        Validates connections and sets up health monitoring.
        """
        await self.caption_balancer.initialize()
        await self.rating_balancer.initialize()
        await self.yolo_balancer.initialize()

    async def release_all_services(self):
        """
        Releases any held services across all balancers.
        Used during cleanup and error scenarios.
        """
        await self.caption_balancer.release_all()
        await self.rating_balancer.release_all()
        await self.yolo_balancer.release_all()

    async def get_services(self, task_id: str) -> Dict[str, str]:
        """Get service URLs for a task"""
        try:
            self.logger.info(f"Worker {self.worker_id} - Getting services for task {task_id}")

            # Get services using async load balancing
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

    async def get_stats(self) -> Dict[str, Any]:
        """
        Returns comprehensive statistics about service usage and health.
        """
        try:
            stats = {
                'caption_services': self.caption_balancer.get_stats(),
                'rating_services': self.rating_balancer.get_stats(),
                'yolo_services': self.yolo_balancer.get_stats(),
                'active_services': {
                    'caption': len(self.caption_balancer.active_services),
                    'rating': len(self.rating_balancer.active_services),
                    'yolo': len(self.yolo_balancer.active_services)
                },
                'worker_info': {
                    'worker_id': self.worker_id,
                    'memory_usage': psutil.Process(self.worker_id).memory_info().rss / 1024 / 1024
                }
            }
            return stats
        except Exception as e:
            self.logger.error(f"Error getting service stats: {str(e)}")
            raise

    async def cleanup(self):
        """Cleanup all resources"""
        await self.yolo_balancer.cleanup()
        await self.caption_balancer.cleanup()
        await self.rating_balancer.cleanup()