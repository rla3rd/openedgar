from typing import Dict, Type, Optional, Union
from .base import BaseFormParser
from .ownership import OwnershipParser
from .thirteenf import ThirteenFParser

class ParserRegistry:
    """
    Registry for managing specialized SEC form parsers.
    """
    def __init__(self):
        self._parsers: Dict[str, BaseFormParser] = {}
        # Auto-register defaults
        self.register(OwnershipParser())
        self.register(ThirteenFParser())

    def register(self, parser: BaseFormParser):
        """Register a parser instance for all form types it handles."""
        for form_type in parser.form_types:
            self._parsers[form_type] = parser

    def get_parser(self, form_type: str) -> Optional[BaseFormParser]:
        """Retrieve the correct parser for a given SEC form type."""
        # Clean form type (e.g. "4/A" or "4")
        clean_type = str(form_type).strip().upper()
        return self._parsers.get(clean_type)

    def to_markdown(self, buffer: Union[bytes, str], form_type: str) -> str:
        """Helper to convert any buffer to markdown using the best available parser."""
        parser = self.get_parser(form_type)
        if parser:
            return parser.to_markdown(buffer)
        
        # Fallback to existing generic hybrid logic if no specialized parser exists
        # This will be handled in openedgar.py
        return None

# Singleton instance
registry = ParserRegistry()
