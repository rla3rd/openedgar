import io
import logging
import sec2md
import pathlib
import os
from typing import Optional, Literal
from docling.document_converter import DocumentConverter
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling_core.types.doc import ImageRefMode

logger = logging.getLogger(__name__)

# Initialize Docling Converter
# We can customize pipeline options here if needed
pdf_options = PdfPipelineOptions()
pdf_options.do_ocr = False  # Faster for digital PDFs, change to True if needed

# Advanced Docling V2 Configuration
converter = DocumentConverter()

def convert_to_markdown(
    buffer: bytes, 
    content_type: str, 
    engine: Literal["auto", "secmd", "docling"] = "auto",
    filename: Optional[str] = None
) -> str:
    """
    Hybrid conversion:
    - HTML -> sec2md (Fast baseline) or docling (Deep analysis)
    - PDF -> docling (High-fidelity layout/table analysis)
    - TXT -> md alias (Workaround for Docling V2 missing enum)
    """
    try:
        # 1. Engine Resolution
        is_html = content_type == "text/html" or (filename and filename.lower().endswith((".htm", ".html")))
        is_pdf = content_type == "application/pdf" or (filename and filename.lower().endswith(".pdf"))
        is_txt = content_type == "text/plain" or (filename and filename.lower().endswith((".txt", ".text")))
        
        # Determine effective engine
        effective_engine = engine
        if engine == "auto":
            if is_html:
                effective_engine = "secmd"
            else:
                effective_engine = "docling"

        # 2. SEcmd Path (Tier 2 - Fast Baseline)
        if effective_engine == "secmd":
            if is_html:
                try:
                    return sec2md.convert_to_markdown(buffer.decode('utf-8', errors='ignore'))
                except Exception as e:
                    logger.warning(f"sec2md failed for {filename}, falling back to docling: {e}")
                    effective_engine = "docling"
            else:
                logger.info(f"Engine 'secmd' requested for non-HTML {filename}, falling back to docling.")
                effective_engine = "docling"

        # 3. Docling Path (Tier 1 - High-Fidelity)
        if effective_engine == "docling":
            from tempfile import NamedTemporaryFile
            
            # Map suffix for Docling format detection
            suffix = ".html"
            if is_pdf:
                suffix = ".pdf"
            elif is_txt:
                suffix = ".md" # ALIAS: Handle .txt as Markdown to bypass missing V2 enum
            elif content_type == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
                suffix = ".docx"
                
            with NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                tmp.write(buffer)
                tmp_path = tmp.name
                
            try:
                result = converter.convert(tmp_path)
                return result.document.export_to_markdown(image_mode=ImageRefMode.REFERENCED)
            finally:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)

    except Exception as e:
        logger.error(f"Hybrid conversion failed for {filename}: {e}")
        return ""

    return ""
