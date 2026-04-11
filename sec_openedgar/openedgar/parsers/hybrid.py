import io
import logging
import sec2md
from docling.document_converter import DocumentConverter
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions

logger = logging.getLogger(__name__)

# Initialize Docling Converter
# We can customize pipeline options here if needed
pdf_options = PdfPipelineOptions()
pdf_options.do_ocr = False  # Faster for digital PDFs, change to True if needed

converter = DocumentConverter()

def convert_to_markdown(buffer: bytes, content_type: str) -> str:
    """
    Hybrid conversion:
    - HTML -> sec2md (optimized for SEC)
    - PDF -> docling (top-tier layout/table analysis)
    - Fallback -> docling (handles HTML/Docx etc with deep analysis)
    """
    try:
        if content_type == "text/html":
            # Primary SEC HTML conversion
            try:
                markdown = sec2md.convert_to_markdown(buffer.decode('utf-8', errors='ignore'))
                # Optional: If table count is high and accuracy is needed, 
                # we could selectively use docling here too.
                return markdown
            except Exception as e:
                logger.warning(f"sec2md failed, falling back to docling: {e}")
        
        # Docling handle
        # For byte buffers, we use it directly
        from docling.datamodel.document import DocumentStream
        
        # We need to wrap buffer in a way docling likes
        # Actually DocumentConverter.convert takes a source (path or URL)
        # For bytes, we use Docling's input mapping
        
        # Simplest way for local bytes:
        from tempfile import NamedTemporaryFile
        import os
        
        suffix = ".html"
        if content_type == "application/pdf":
            suffix = ".pdf"
        elif content_type == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
            suffix = ".docx"
            
        with NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(buffer)
            tmp_path = tmp.name
            
        try:
            result = converter.convert(tmp_path)
            return result.document.export_to_markdown()
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    except Exception as e:
        logger.error(f"Hybrid conversion failed: {e}")
        return ""

