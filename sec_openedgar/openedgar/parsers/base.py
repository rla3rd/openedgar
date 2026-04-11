from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Union

class BaseFormParser(ABC):
    """
    Abstract Base Class for all specialized SEC form parsers.
    Each implementation provides:
    1. A way to generate "Visual Markdown" for LLM consumption.
    2. A way to extract "Ground Truth" data for benchmarking.
    """
    
    @abstractmethod
    def to_markdown(self, buffer: Union[bytes, str]) -> str:
        """Converts raw filing content to cleaned, structural Markdown."""
        pass

    @abstractmethod
    def extract_ground_truth(self, buffer: Union[bytes, str]) -> Dict[str, Any]:
        """Extracts 100% accurate structured data from the source (Rules-based)."""
        pass

    @property
    @abstractmethod
    def form_types(self) -> List[str]:
        """The list of SEC form types this parser handles (e.g. ['4', '4/A'])."""
        pass
