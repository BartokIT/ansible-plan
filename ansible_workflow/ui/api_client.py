import httpx
import logging
from typing import List, Dict, Any, Optional

class ApiClient:
    def __init__(self, base_url: str, logger=None):
        self.base_url = base_url
        self.client = httpx.Client(base_url=self.base_url)
        self.logger = logger or logging.getLogger(__name__)

    def get_workflow_status(self) -> Optional[Dict[str, Any]]:
        try:
            response = self.client.get("/workflow")
            response.raise_for_status()
            return response.json()
        except (httpx.ConnectError, httpx.HTTPStatusError):
            return None

    def get_all_nodes(self) -> Optional[List[Dict[str, Any]]]:
        try:
            response = self.client.get("/workflow/nodes")
            response.raise_for_status()
            return response.json()
        except (httpx.ConnectError, httpx.HTTPStatusError):
            return None

    def get_workflow_graph(self) -> Optional[List[List[str]]]:
        try:
            response = self.client.get("/workflow/graph")
            response.raise_for_status()
            return response.json()["edges"]
        except (httpx.ConnectError, httpx.HTTPStatusError):
            return None

    def get_node_stdout(self, node_id: str) -> Optional[str]:
        try:
            response = self.client.get(f"/workflow/node/{node_id}/stdout")
            response.raise_for_status()
            return response.json()["stdout"]
        except (httpx.ConnectError, httpx.HTTPStatusError):
            return None

    def stop_workflow(self, mode: str = "graceful"):
        try:
            response = self.client.post("/workflow/stop", json={"mode": mode})
            response.raise_for_status()
        except (httpx.ConnectError, httpx.HTTPStatusError):
            pass

    def pause_workflow(self):
        self.logger.info("Pausing workflow via API")
        try:
            response = self.client.post("/workflow/pause")
            response.raise_for_status()
        except (httpx.ConnectError, httpx.HTTPStatusError) as e:
            self.logger.error(f"Failed to pause workflow: {e}")
            pass

    def resume_workflow(self):
        self.logger.info("Resuming workflow via API")
        try:
            response = self.client.post("/workflow/resume")
            response.raise_for_status()
        except (httpx.ConnectError, httpx.HTTPStatusError) as e:
            self.logger.error(f"Failed to resume workflow: {e}")
            pass

    def shutdown_backend(self):
        try:
            self.client.post("/shutdown")
        except (httpx.ReadError, httpx.ConnectError, httpx.HTTPStatusError):
            # This is expected as the server will shut down before sending a response
            pass

    def restart_node(self, node_id: str):
        try:
            response = self.client.post(f"/workflow/node/{node_id}/restart")
            response.raise_for_status()
        except (httpx.ConnectError, httpx.HTTPStatusError):
            pass

    def skip_node(self, node_id: str):
        try:
            response = self.client.post(f"/workflow/node/{node_id}/skip")
            response.raise_for_status()
        except (httpx.ConnectError, httpx.HTTPStatusError):
            pass

    def approve_node(self, node_id: str):
        try:
            response = self.client.post(f"/workflow/node/{node_id}/approve")
            response.raise_for_status()
        except (httpx.ConnectError, httpx.HTTPStatusError):
            pass

    def disapprove_node(self, node_id: str):
        try:
            response = self.client.post(f"/workflow/node/{node_id}/disapprove")
            response.raise_for_status()
        except (httpx.ConnectError, httpx.HTTPStatusError):
            pass

    def check_health(self) -> bool:
        try:
            response = self.client.get("/health")
            response.raise_for_status()
            return True
        except (httpx.ConnectError, httpx.HTTPStatusError):
            return False
