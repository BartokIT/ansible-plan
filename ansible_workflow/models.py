from pydantic import BaseModel, Field
from typing import Optional, Dict, List

class WorkflowStartRequest(BaseModel):
    workflow_file: str
    extra_vars: Dict = Field(default_factory=dict)
    input_templating: Dict = Field(default_factory=dict)
    check_mode: bool = False
    verbosity: int = 0
    start_from_node: Optional[str] = None
    end_to_node: Optional[str] = None
    skip_nodes: List[str] = Field(default_factory=list)
    filter_nodes: List[str] = Field(default_factory=list)
    log_dir: str = "logs"
    log_dir_no_info: bool = False
    log_level: str = "info"
    verify_only: bool = False
