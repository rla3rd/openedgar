# This module re-exports the canonical parser so that research scripts can import
# from either path without diverging implementations (see steering.md: DRY).
from openedgar.parsers.ownership_parser import OwnershipParser as OwnershipMarkdownSynthesizer

__all__ = ['OwnershipMarkdownSynthesizer']
