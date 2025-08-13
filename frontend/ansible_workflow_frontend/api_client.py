import httpx
from typing import List, Dict, Any

class ApiClient:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.client = httpx.Client(base_url=self.base_url)

    def get_workflow_status(self) -> str:
        response = self.client.get("/workflow")
        response.raise_for_status()
        return response.json()["status"]

    def get_all_nodes(self) -> List[Dict[str, Any]]:
        response = self.client.get("/workflow/nodes")
        response.raise_for_status()
        return response.json()

    def get_workflow_graph(self) -> List[List[str]]:
        response = self.client.get("/workflow/graph")
        response.raise_for_status()
        return response.json()["edges"]

    def get_node_stdout(self, node_id: str) -> str:
        response = self.client.get(f"/workflow/node/{node_id}/stdout")
        response.raise_for_status()
        return response.json()["stdout"]

    def stop_workflow(self):
        response = self.client.post("/workflow/stop")
        response.raise_for_status()

    def shutdown_backend(self):
        try:
            self.client.post("/shutdown")
        except httpx.ReadError:
            # This is expected as the server will shut down before sending a response
            pass

    def restart_node(self, node_id: str):
        response = self.client.post(f"/workflow/node/{node_id}/restart")
        response.raise_for_status()

    def skip_node(self, node_id: str):
        response = self.client.post(f"/workflow/node/{node_id}/skip")
        response.raise_for_status()
