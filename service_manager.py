# service_manager.py
import threading
from dataclasses import dataclass
from datetime import datetime
from itertools import cycle
from typing import Dict, List, Optional
from threading import Lock
import logging

@dataclass
class ServiceConfig:
    """Configuration for a service endpoint"""
    port: str
    gpu: str
    health_check_url: Optional[str] = None
    last_health_check: Optional[datetime] = None
    is_healthy: bool = True

    def get_url(self, base_url: str = "http://localhost", endpoint: str = "") -> str:
        """Generate full service URL"""
        return f"{base_url}:{self.port}{endpoint}"

class ServiceUsageStats:
    """Track service usage statistics"""
    def __init__(self):
        self.total_requests: int = 0
        self.failed_requests: int = 0
        self.last_used: Optional[datetime] = None
        self.last_error: Optional[str] = None
        self.average_response_time: float = 0.0
        self.total_response_time: float = 0.0

    def update_stats(self, response_time: float, failed: bool = False):
        """Update service statistics after a request"""
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
    """Enhanced load balancer with monitoring and health checks"""
    def __init__(self, services: List[Dict[str, str]], endpoint: str):
        self.configs = [ServiceConfig(**svc) for svc in services]
        self.endpoint = endpoint
        self._service_cycle = cycle(self.configs)
        self.lock = threading.Lock()
        self.stats = {svc.port: ServiceUsageStats() for svc in self.configs}
        self.logger = logging.getLogger(__name__)

    def get_next_service(self) -> ServiceConfig:
        """Get next available healthy service"""
        with self.lock:
            # Try up to len(configs) times to find a healthy service
            for _ in range(len(self.configs)):
                service = next(self._service_cycle)
                if service.is_healthy:
                    self.stats[service.port].total_requests += 1
                    self.stats[service.port].last_used = datetime.now()
                    return service
            # If no healthy service found, use the next available one
            service = next(self._service_cycle)
            self.logger.warning(f"No healthy services available, using {service.port}")
            return service

    def mark_error(self, port: str, error: str):
        """Record service error and update health status"""
        with self.lock:
            if port not in self.stats:
                return
            self.stats[port].failed_requests += 1
            self.stats[port].last_error = error
            service = next(s for s in self.configs if s.port == port)
            service.is_healthy = False
            self.logger.error(f"Service {port} marked unhealthy: {error}")

    def mark_success(self, port: str, response_time: float):
        """Record successful request"""
        with self.lock:
            if port not in self.stats:
                return
            stats = self.stats[port]
            stats.update_stats(response_time, failed=False)
            service = next(s for s in self.configs if s.port == port)
            service.is_healthy = True

    def get_stats(self) -> Dict[str, Dict]:
        """Get current service statistics"""
        with self.lock:
            return {
                port: {
                    "total_requests": stats.total_requests,
                    "failed_requests": stats.failed_requests,
                    "last_used": stats.last_used.isoformat() if stats.last_used else None,
                    "last_error": stats.last_error,
                    "average_response_time": stats.average_response_time,
                    "is_healthy": next(
                        s.is_healthy for s in self.configs if s.port == port
                    )
                }
                for port, stats in self.stats.items()
            }

class ServiceManager:
    """Central manager for all service types"""
    def __init__(
        self,
        yolo_services: List[Dict[str, str]],
        caption_services: List[Dict[str, str]],
        rating_services: List[Dict[str, str]]
    ):
        self.yolo_balancer = ServiceBalancer(yolo_services, "/detect_batch_folder")
        self.caption_balancer = ServiceBalancer(caption_services, "/upload")
        self.rating_balancer = ServiceBalancer(rating_services, "/api")
        self.logger = logging.getLogger(__name__)

    def get_services(self, task_id: str) -> Dict[str, str]:
        """Get URLs for all required services for a task"""
        self.logger.info(f"Getting services for task {task_id}")
        yolo_service = self.yolo_balancer.get_next_service()
        caption_service = self.caption_balancer.get_next_service()
        rating_service = self.rating_balancer.get_next_service()

        return {
            "yolo_url": yolo_service.get_url(endpoint="/detect_batch_folder"),
            "caption_url": caption_service.get_url(endpoint="/upload"),
            "rating_url": rating_service.get_url(endpoint="/api")
        }

    def mark_service_error(self, service_type: str, port: str, error: str):
        """Record service error"""
        balancer = getattr(self, f"{service_type}_balancer", None)
        if balancer:
            balancer.mark_error(port, error)
            self.logger.error(f"{service_type.upper()} service {port} error: {error}")

    def mark_service_success(self, service_type: str, port: str, response_time: float):
        """Record successful service request"""
        balancer = getattr(self, f"{service_type}_balancer", None)
        if balancer:
            balancer.mark_success(port, response_time)

    def get_all_stats(self) -> Dict[str, Dict]:
        """Get statistics for all services"""
        return {
            "yolo": self.yolo_balancer.get_stats(),
            "caption": self.caption_balancer.get_stats(),
            "rating": self.rating_balancer.get_stats()
        }