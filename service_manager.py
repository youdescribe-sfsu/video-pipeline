import logging
import os
import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional, Any
import aiohttp
import psutil
from queue_config import global_task_queue, caption_queue
from web_server_module.web_server_database import get_status_for_youtube_id, get_module_output


@dataclass
class ServiceConfig:
    """
    Configuration for a single service endpoint.
    Each service runs on a dedicated port with dedicated GPU resources.
    """
    port: str
    gpu: str
    endpoint: str
    is_healthy: bool = True

    def get_url(self, base_url: str = "http://localhost") -> str:
        """Construct the complete service URL"""
        return f"{base_url}:{self.port}{self.endpoint}"

    def __str__(self) -> str:
        """Human-readable service representation"""
        return f"Service on port {self.port} using GPU {self.gpu}"


class ServiceMonitor:
    """
    Monitors the health and status of a single service instance.
    Provides health checking and basic statistics tracking.
    """

    def __init__(self, service: ServiceConfig):
        self.service = service
        self.last_check = None
        self.total_requests = 0
        self.failed_requests = 0
        self.session: Optional[aiohttp.ClientSession] = None
        self.logger = logging.getLogger(__name__)

    async def initialize(self):
        """Initialize monitoring session for health checks"""
        if self.session is None:
            self.session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(force_close=True)
            )

    async def check_health(self) -> Dict[str, Any]:
        """
        Check if the service is responding properly.
        Returns detailed health status information.
        """
        if not self.session:
            await self.initialize()

        try:
            url = self.service.get_url()
            async with self.session.get(url, timeout=2) as response:
                # 405 is healthy for services that only accept POST
                is_healthy = response.status in (405, 404, 200)
                self.service.is_healthy = is_healthy
                self.last_check = datetime.now()

                return {
                    'healthy': is_healthy,
                    'gpu': self.service.gpu,
                    'status_code': response.status,
                    'last_check': self.last_check.isoformat(),
                    'total_requests': self.total_requests,
                    'failed_requests': self.failed_requests
                }

        except Exception as e:
            self.logger.error(f"Health check failed for {url}: {str(e)}")
            self.service.is_healthy = False
            return {
                'healthy': False,
                'gpu': self.service.gpu,
                'error': str(e),
                'last_check': datetime.now().isoformat()
            }

    def record_request(self, failed: bool = False):
        """Record request statistics"""
        self.total_requests += 1
        if failed:
            self.failed_requests += 1

    async def cleanup(self):
        """Clean up monitoring resources"""
        if self.session:
            await self.session.close()
            self.session = None


class ServiceManager:
    """
    Manages individual service instances for the video processing pipeline.
    Each service type (caption, rating, YOLO) has exactly one dedicated instance.
    """

    def __init__(self):
        # Initialize single service instances
        self.caption_service = ServiceConfig(
            port="8085",
            gpu="4",
            endpoint="/upload"
        )
        self.rating_service = ServiceConfig(
            port="8082",
            gpu="4",
            endpoint="/api"
        )
        self.yolo_service = ServiceConfig(
            port="8087",
            gpu="2",
            endpoint="/detect_batch_folder"
        )

        # Initialize monitors for each service
        self.caption_monitor = ServiceMonitor(self.caption_service)
        self.rating_monitor = ServiceMonitor(self.rating_service)
        self.yolo_monitor = ServiceMonitor(self.yolo_service)

        # Queue references from central configuration
        self.caption_queue = caption_queue
        self.global_queue = global_task_queue

        self.logger = logging.getLogger(__name__)
        self._initialized = False

    def is_caption_task(self, task_data: Dict[str, Any]) -> bool:
        """
        Determine if a task should be processed by the caption service.
        Checks task type and validates prerequisites.
        """
        if task_data.get("task_type") != "image_captioning":
            return False

        video_id = task_data.get("youtube_id")
        ai_user_id = task_data.get("ai_user_id")

        if not all([video_id, ai_user_id]):
            self.logger.warning("Missing required fields for caption task validation")
            return False

        if not self.check_caption_prerequisites(video_id, ai_user_id):
            self.logger.info(f"Prerequisites not met for captioning task {video_id}")
            return False

        if get_status_for_youtube_id(video_id, ai_user_id) == "done":
            self.logger.info(f"Captioning already completed for {video_id}")
            return False

        return True

    def check_caption_prerequisites(self, video_id: str, ai_user_id: str) -> bool:
        """Verify all required processing steps are complete before captioning"""
        required_outputs = ["frame_extraction", "object_detection", "keyframe_selection"]
        return all(
            get_module_output(video_id, ai_user_id, module) is not None
            for module in required_outputs
        )

    async def initialize(self):
        """Initialize all service monitors and verify health"""
        if self._initialized:
            return

        try:
            # Initialize all monitors
            await asyncio.gather(
                self.caption_monitor.initialize(),
                self.rating_monitor.initialize(),
                self.yolo_monitor.initialize()
            )

            # Perform initial health check
            health_status = await self.check_all_services_health()
            if not health_status['overall_health']['healthy']:
                unhealthy = [name for name, status in health_status.items()
                             if not status.get('healthy', False)]
                raise RuntimeError(f"Unhealthy services: {', '.join(unhealthy)}")

            self._initialized = True
            self.logger.info("Service manager initialized successfully")

        except Exception as e:
            await self.cleanup()
            raise RuntimeError(f"Service initialization failed: {str(e)}")

    async def check_all_services_health(self) -> Dict[str, Any]:
        """Check health status of all services"""
        health_status = {
            'caption': await self.caption_monitor.check_health(),
            'rating': await self.rating_monitor.check_health(),
            'yolo': await self.yolo_monitor.check_health()
        }

        health_status['overall_health'] = {
            'healthy': all(status['healthy'] for status in health_status.values()),
            'timestamp': datetime.now().isoformat()
        }

        return health_status

    async def get_services(self, task_id: str) -> Dict[str, str]:
        """Get URLs for all required services"""
        self.logger.info(f"Getting service URLs for task {task_id}")

        return {
            "caption_url": self.caption_service.get_url(),
            "rating_url": self.rating_service.get_url(),
            "yolo_url": self.yolo_service.get_url()
        }

    async def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive service statistics"""
        try:
            return {
                'caption': await self.caption_monitor.check_health(),
                'rating': await self.rating_monitor.check_health(),
                'yolo': await self.yolo_monitor.check_health(),
                'worker_info': {
                    'pid': os.getpid(),
                    'memory_mb': psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
                }
            }
        except Exception as e:
            self.logger.error(f"Error getting service stats: {str(e)}")
            raise

    async def cleanup(self):
        """Clean up all service monitors"""
        await asyncio.gather(
            self.caption_monitor.cleanup(),
            self.rating_monitor.cleanup(),
            self.yolo_monitor.cleanup()
        )