"""
MIT License

Copyright (c) 2018 ContraxSuite, LLC

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

# Libraries
import binascii
import datetime
import gzip
import hashlib
import io
import logging
import mimetypes
import re
import os
import zlib
from typing import Union

# Packages
import dateutil.parser
import pandas
import sec2md
from docling.document_converter import DocumentConverter
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions
import tempfile
from .registry import registry
import secsgml2

# Setup logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logger.addHandler(console)


def uudecode(buffer: Union[bytes, str]):
    """
    uudecode an input buffer; based on python library uu but with support for byte stream
    :param buffer:
    :return:
    """
    # Create in_file from buffer
    in_file = io.BytesIO(buffer)
    out_file = io.BytesIO()

    while True:
        hdr = in_file.readline()
        if not hdr.startswith(b'begin'):
            continue
        hdrfields = hdr.split(b' ', 2)
        if len(hdrfields) == 3 and hdrfields[0] == b'begin':
            try:
                int(hdrfields[1], 8)
                break
            except ValueError:
                pass

    s = in_file.readline()
    while s and s.strip(b' \t\r\n\f') != b'end':
        try:
            data = binascii.a2b_uu(s)
        except binascii.Error as _:
            # Workaround for broken uuencoders by /Fredrik Lundh
            nbytes = (((s[0] - 32) & 63) * 4 + 5) // 3
            data = binascii.a2b_uu(s[:nbytes])
        out_file.write(data)
        s = in_file.readline()

    return out_file.getvalue()


# Initialize Docling Converter Lazily (to avoid CUDA multiprocessing issues)
_converter = None

def get_converter():
    global _converter
    if _converter is None:
        from docling.document_converter import DocumentConverter
        _converter = DocumentConverter()
    return _converter

def extract_text(buffer: Union[bytes, str], content_type: str = None, form_type: str = None):
    """
    Extract text/markdown from a document using a hybrid pipeline:
    - Registry: Rules-based specialized parsers (e.g. Ownership)
    - HTML: sec2md (optimized for SEC structure)
    - PDF/Complex: Docling (layout-aware parsing)
    """
    try:
        if isinstance(buffer, str):
            buffer = buffer.encode('utf-8')
        
        # 1. Check specialized parser registry first
        if form_type:
            specialized_md = registry.to_markdown(buffer, form_type)
            if specialized_md:
                return specialized_md
            
        # 2. Hybrid Logic (HTML/PDF/XML)
        if content_type == "text/html":
            decoded = buffer.decode('utf-8', errors='ignore')
            try:
                # Ultra-fast path using C-based Modest engine (selectolax)
                from selectolax.parser import HTMLParser
                tree = HTMLParser(decoded)
                # Strip out style and script tags completely
                for node in tree.css('script, style, meta, head'):
                    node.decompose()
                return tree.body.text(separator=' ', strip=True) if tree.body else tree.text(strip=True)
            except ImportError:
                try:
                    # Optimized for primary SEC filings but significantly slower
                    return sec2md.convert_to_markdown(decoded)
                except Exception as e:
                    logger.warning(f"sec2md conversion failed, falling back to docling: {e}")
        
        elif "xml" in str(content_type).lower():
            # For XML, we often just want the clean text if it's not a specialized form
            try:
                decoded = buffer.decode('utf-8', errors='ignore')
                from selectolax.parser import HTMLParser
                tree = HTMLParser(decoded)
                return tree.text(separator=' ', strip=True)
            except Exception as e:
                logger.warning(f"XML text extraction failed: {e}")

        # Docling handles PDF, HTML fallback, Docx, etc with deep layout analysis
        suffix = ".html"
        if content_type == "application/pdf":
            suffix = ".pdf"
        elif "xml" in str(content_type).lower():
            # If we fall through to docling for XML, it might fail anyway, 
            # but we'll try it as a last resort if it matches docling's supported formats
            suffix = ".xml"
            
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(buffer)
            tmp_path = tmp.name
            
        try:
            conv = get_converter()
            result = conv.convert(tmp_path)
            return result.document.export_to_markdown()
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    except Exception as e:
        logger.error(f"Text extraction failed ({content_type}): {e}")
        return ""


def parse_index_file(file_name: str, double_gz: bool = False):
    """
    Parse an index file.
    :param file_name:
    :param double_gz:
    :return:
    """
    # Log entrance
    if not os.path.exists(file_name):
        if os.path.exists(file_name + ".gz"):
            file_name += ".gz"
        else:
            logger.error("File {0} does not exist on filesystem.".format(file_name))
            return pandas.DataFrame()

    logger.info("Parsing index file: {0}".format(file_name))

    # Read index
    try:
        with gzip.open(file_name, "rb") as index_file:
            index_buffer = index_file.read()
    except IOError as e:
        logger.error("IOError parsing {0}: {1}".format(file_name, e))
        # Read as plain binary
        with open(file_name, "rb") as index_file:
            index_buffer = index_file.read()

        # Check for alternative header
        if index_buffer[0] == '\x78' and (ord(index_buffer[1]) + 0x7800) % 31 == 0:
            index_buffer = zlib.decompress(index_buffer).decode("utf-8")
            logger.info("gz with valid header: decompressing {0} to {1} bytes.".format(file_name,
                                                                                       len(index_buffer)))

        # Check for double-gz
        if double_gz:
            index_buffer = os.popen("gunzip -c {0} | gunzip -c".format(file_name)).read() \
                .decode("utf-8", "ignore").decode("utf-8", "ignore")
            logger.warning("Double-decompressing buffer for {0}".format(file_name))

    # Re-code to UTF-8
    try:
        index_buffer = index_buffer.decode("utf-8")
    except UnicodeDecodeError as _:
        # Check for double-compression
        try:
            index_buffer = gzip.GzipFile(fileobj=io.BytesIO(index_buffer)).read().encode("utf-8").decode("utf-8")
        except UnicodeDecodeError as _:
            try:
                index_buffer = os.popen("gunzip -c {0} | gunzip -c".format(file_name)).read() \
                    .decode("utf-8", "ignore").decode("utf-8", "ignore")
                logger.warning("Double-decompressing buffer for {0}".format(file_name))
            except UnicodeDecodeError as g:
                logger.error("Error decoding {0}: {1}".format(file_name, g))
                logger.error("First 10 bytes: {0}".format(index_buffer[0:10]))
                return pandas.DataFrame()
            except OSError as h:
                logger.error("Error decoding {0}: {1}".format(file_name, h))
                logger.error("First 10 bytes: {0}".format(index_buffer[0:10]))
                return pandas.DataFrame()
        except OSError as h:
            logger.error("Error decoding {0}: {1}".format(file_name, h))
            logger.error("First 10 bytes: {0}".format(index_buffer[0:10]))
            return pandas.DataFrame()

    # Get header line and data line starts
    header_line_pos = index_buffer.find("\nForm Type") + 1
    separator_line_pos = index_buffer.find("-", header_line_pos + 1)
    data_line_pos = index_buffer.find("\n", separator_line_pos + 1)

    # Build buffer and parse as fixed-width file
    data_buffer = io.StringIO(index_buffer[header_line_pos:separator_line_pos].replace("\n", "\t")
                              + index_buffer[data_line_pos:])
    data_table = pandas.read_fwf(data_buffer,
                                 colspecs="infer",
                                 encoding="utf-8")

    # Deal with broken field names
    if "Form" in data_table.columns and "Form Type" not in data_table.columns:
        logger.warning("Index file has abnormal columns: {0}".format(file_name))
        data_table["Form Type"] = data_table["Form"]
        del data_table["Form"]

    # Remove unknown field names
    good_columns = ["CIK", "Company Name", "Date Filed", "File Name", "Form Type"]
    try:
        data_table = data_table.loc[:, good_columns]
    except KeyError:
        logger.error("Unable to identify proper columns in {0}".format(file_name))
        logger.error("Columns found: {0}".format(data_table.columns))

    # Log exit
    logger.info("Completed parsing index file: {0}".format(file_name))
    logger.info("Index data shape: {0}".format(data_table.shape))

    # Return
    return data_table


def extract_filing_header_field(buffer: Union[bytes, str], field: str):
    """
    Extract a given field from an SEC-HEADER buffer using robust regex.
    Handles 'FIELD: VALUE', 'FIELD \t VALUE', 'FIELD-NAME: VALUE', and '<FIELD-NAME>VALUE'.
    """
    import re
    if isinstance(buffer, bytes):
        buffer = buffer.decode("utf-8", errors="ignore")
    
    # Normalize field for regex (handle spaces as spaces or hyphens)
    field_slug = field.replace(" ", r"[ -]")
    
    # Try multiple patterns: 
    # 1. <FIELD>VALUE
    # 2. FIELD: VALUE
    # 3. FIELD  VALUE (spaces)
    # We allow optional leading whitespace and handle the closing tag char '>'
    patterns = [
        rf"<{field_slug}>\s*(.*)$",              # SGML Tag (Priority)
        rf"^\s*(?:{field_slug})\s*[:>]?\s*(.*)$", # Line start + optional colon/tag-end
        rf"(?:{field_slug})\s*[:>]?\s*(.*)$",      # Mid-line (fallback)
    ]
    
    for pattern in patterns:
        match = re.search(pattern, buffer, re.IGNORECASE | re.MULTILINE)
        if match:
            val = match.group(1).split("\n")[0].strip()
            # If we caught a tag-based value, strip the closing tag if it exists on the same line
            val = re.sub(r"</.*?>", "", val).strip()
            # Final safety strip for tag remnants
            val = val.lstrip(">").strip()
            if val:
                return val
    return None


def parse_filing(buffer: Union[bytes, str], extract: bool = False):
    """
    Parse a filing file by returning each document within
    :param buffer:
    :param extract: whether to extract raw text
    :return:
    """
    # Start and end tags
    start_tag = "<DOCUMENT>"
    end_tag = "</DOCUMENT>"
    filing_data = {"documents": [],
                   "accession_number": None,
                   "form_type": None,
                   "document_count": None,
                   "reporting_period": None,
                   "date_filed": None,
                   "company_name": None,
                   "cik": None,
                   "sic": None,
                   "trading_symbol": None,
                   "irs_number": None,
                   "state_incorporation": None,
                   "state_location": None}

    # Typing
    if isinstance(buffer, bytes):
        try:
            # Start with UTF-8
            buffer = str(buffer.decode("utf-8"))
        except UnicodeDecodeError as _:
            try:
                # Fallback to ISO 8859-1
                logger.warning("Falling back to ISO 8859-1 after failing to decode with UTF-8...")
                buffer = str(buffer.decode("iso-8859-1"))
            except UnicodeDecodeError as _:
                # Give up if we can't
                logger.error("Unable to decode with either UTF-8 or ISO 8859-1; giving up...")
                return filing_data

    # Check for SEC-HEADER block
    # Check for SEC-HEADER block with fallback
    header_p0 = -1
    header_p1 = -1
    tag_len = 0
    
    if "<SEC-HEADER>" in buffer:
        header_p0 = buffer.find("<SEC-HEADER>")
        header_p1 = buffer.find("</SEC-HEADER>")
        tag_len = len("<SEC-HEADER>")
    elif "<IMS-HEADER>" in buffer:
        header_p0 = buffer.find("<IMS-HEADER>")
        header_p1 = buffer.find("</IMS-HEADER>")
        tag_len = len("<IMS-HEADER>")
    
    # If no tags, fallback to the area before the first <DOCUMENT>
    if header_p0 == -1 or header_p1 == -1:
        header_p0 = 0
        header_p1 = buffer.find("<DOCUMENT>")
        tag_len = 0
        
    if header_p1 == -1: # No documents?! Just use the whole thing
        header_p1 = len(buffer)
        
    # Extract and Parse valid header
    header = buffer[header_p0 + tag_len:header_p1]
    
    # Extract and Parse valid header using high-performance secsgml2
    header_bytes = header.encode("utf-8", errors="ignore")
    raw_metadata, _ = secsgml2.parse_sgml_content_into_memory(header_bytes)
    
    # 0. Sanitize metadata (PostgreSQL jsonb does not allow null bytes \u0000)
    def sanitize(val, is_key=False):
        if isinstance(val, dict):
            new_dict = {}
            for k, v in val.items():
                k_clean = sanitize(k, is_key=True)
                
                # ULTIMATE FIX: Proactive Accession Number detection by value pattern
                # If the value looks like an ACN, force the key to 'accession-number'
                if isinstance(v, str) and re.match(r"^\d{10}-\d{2}-\d{6}$", v):
                    k_clean = "accession-number"
                elif re.match(r"^[^\w\s-]{2,}", k_clean) or not k_clean or len(k_clean) < 3:
                    # If the key is garbage/mangled and we haven't identified it, 
                    # we should be careful about keeping it if it has nulls or binary
                    pass

                new_dict[k_clean] = sanitize(v)
            return new_dict
        elif isinstance(val, list):
            return [sanitize(i) for i in val]
        elif isinstance(val, str):
            # Strip nulls, non-printable, and non-ASCII
            res = val.replace('\x00', '').replace('\u0000', '')
            if is_key:
                res = "".join(c for c in res if c.isprintable() and ord(c) < 128)
            return res.strip()
        return val
    
    metadata = sanitize(raw_metadata)

    # Map secsgml2 metadata (handling hyphenated keys and nested structures)
    # 1. Robust Accession Number recovery (handles binary key mangling)
    acn = metadata.get("accession-number") or metadata.get("ACCESSION-NUMBER")
    if not acn:
        # Fallback: Search all values for something that looks like an ACN
        for k, v in metadata.items():
            if isinstance(v, str) and re.match(r"^\d{10}-\d{2}-\d{6}$", v):
                acn = v
                break
    filing_data["accession_number"] = acn

    filing_data["form_type"] = (metadata.get("type") or 
                               metadata.get("conformed-submission-type") or 
                               metadata.get("TYPE"))
    
    doc_count = metadata.get("public-document-count") or metadata.get("PUBLIC-DOCUMENT-COUNT")
    try:
        filing_data["document_count"] = int(doc_count) if doc_count else None
    except (ValueError, TypeError):
        filing_data["document_count"] = None

    period = (metadata.get("conformed-period-of-report") or 
              metadata.get("period") or 
              metadata.get("PERIOD"))
    if period:
        try:
            filing_data["reporting_period"] = dateutil.parser.parse(str(period)).date()
        except Exception:
            filing_data["reporting_period"] = None

    filed = (metadata.get("filing-date") or 
             metadata.get("filed-as-of-date") or 
             metadata.get("FILING_DATE") or
             metadata.get("DATE_FILED"))
    if filed:
        try:
            filing_data["date_filed"] = dateutil.parser.parse(str(filed)).date()
        except Exception:
            filing_data["date_filed"] = None

    # 2. Extract Entity Data (Prioritize SUBJECT-COMPANY for issuers, fall back to FILED-BY)
    entities = [metadata.get("subject-company"), metadata.get("filed-by"), metadata.get("reporting-owner")]
    for entity in entities:
        if not entity or not isinstance(entity, dict):
            continue
        
        cdata = entity.get("company-data") or entity.get("owner-data") or {}
        if not cdata:
            continue
            
        filing_data["company_name"] = cdata.get("conformed-name") or cdata.get("name")
        filing_data["cik"] = cdata.get("cik")
        filing_data["sic"] = cdata.get("assigned-sic") or cdata.get("sic")
        filing_data["irs_number"] = cdata.get("irs-number") or cdata.get("irs")
        filing_data["state_incorporation"] = cdata.get("state-of-incorporation") or cdata.get("state")
        
        # If we found the primary issuer, stop
        if filing_data["company_name"]:
            break

    # Final cleanup of CIKs (padding to 10 digits)
    if filing_data["cik"]:
        filing_data["cik"] = str(filing_data["cik"]).zfill(10)

    # Include full raw metadata    # Capture the raw SGML header ONLY for the sidecar
    start_tag_bytes = "<DOCUMENT>"
    p0 = buffer.find(start_tag_bytes)
    header = buffer[:p0] if p0 != -1 else buffer

    filing_data["raw_header"] = header
    filing_data["full_metadata"] = metadata
    
    # Enrich filing documents with secsgml2 metadata (like size_bytes)
    # Note: filing_data["documents"] already contains parsed docs, 
    # but we want to ensure the secsgml2-specific 'documents' list is available 
    # for the offset calculator.
    filing_data["secsgml_documents"] = metadata.get("documents", [])

    # Parse and yield by doc
    p0 = buffer.find(start_tag)
    while p0 != -1:
        p1 = buffer.find(end_tag, p0)

        # Parse document
        document_buffer = buffer[p0:(p1 + len(end_tag))]
        document_data = parse_filing_document(document_buffer, extract=extract, form_type=filing_data["form_type"])
        document_data["start_pos"] = p0
        document_data["end_pos"] = (p1 + len(end_tag))
        filing_data["documents"].append(document_data)
        p0 = buffer.find(start_tag, p1)

    return filing_data


def parse_filing_document(document_buffer: Union[bytes, str], extract: bool = False, form_type: str = None):
    """
    Parse a document buffer into metadata and contents.
    :param document_buffer: raw document buffer
    :param extract: whether to pass to Tika for text extraction
    :param form_type: SEC form type for registry routing
    :return:
    """
    # Robust regex for document metadata tags
    import re
    doc_type_matches = re.findall(r"<TYPE>\s*(.*)", document_buffer, re.IGNORECASE)
    doc_type = doc_type_matches[0].strip() if doc_type_matches else form_type
    
    doc_sequence_matches = re.findall(r"<SEQUENCE>\s*(.*)", document_buffer, re.IGNORECASE)
    doc_sequence = doc_sequence_matches[0].strip() if doc_sequence_matches else None
    
    doc_file_name_matches = re.findall(r"<FILENAME>\s*(.*)", document_buffer, re.IGNORECASE)
    doc_file_name = doc_file_name_matches[0].strip() if doc_file_name_matches else None
    
    doc_description_matches = re.findall(r"<DESCRIPTION>\s*(.*)", document_buffer, re.IGNORECASE)
    doc_description = doc_description_matches[0].strip() if doc_description_matches else None

    # Start and end tags
    content_p0 = document_buffer.rfind("</", 0, document_buffer.rfind("</"))
    content_p1 = document_buffer.find(">", content_p0)
    doc_tag_type = document_buffer[content_p0 + len("</"):content_p1]
    content_start_tag = "<{0}>".format(doc_tag_type)
    content_end_tag = "</{0}>".format(doc_tag_type)

    doc_content_p0 = document_buffer.find(content_start_tag) + len(content_start_tag)
    doc_content_p1 = document_buffer.find(content_end_tag)
    doc_content = document_buffer[doc_content_p0:doc_content_p1]

    # Check content types
    is_uuencoded = False
    doc_text_head = doc_content[0:100]
    doc_text_head_upper = doc_text_head.upper()

    if "<PDF>" in doc_text_head_upper:
        is_uuencoded = True
        content_type = "application/pdf"
    elif "<HTML" in doc_text_head_upper:
        content_type = "text/html"
    elif "<XML" in doc_text_head_upper:
        content_type = "application/xml"
    elif "<?XML" in doc_text_head_upper:
        content_type = "application/xml"
    elif doc_text_head.startswith("\nbegin "):
        is_uuencoded = True
        if doc_file_name:
            content_type = mimetypes.guess_type(os.path.basename(doc_file_name))
            if content_type is None:
                content_type = "application/octet-stream"
            else:
                content_type = content_type[0]
        else:
            content_type = "application/octet-stream"
    else:
        content_type = "text/plain"

    # uudecode if required and calculate hash for sharding/dedupe
    doc_content = doc_content.encode("utf-8")
    if is_uuencoded:
        doc_content = uudecode(doc_content)
    doc_sha1 = hashlib.sha1(doc_content).hexdigest()

    # extract text using hybrid pipeline if requested
    if extract:
        doc_content_text = extract_text(doc_content, content_type=content_type, form_type=doc_type)
    else:
        doc_content_text = None

    return {"type": doc_type,
            "sequence": doc_sequence,
            "file_name": doc_file_name,
            "description": doc_description,
            "content_type": content_type,
            "sha1": doc_sha1,
            "content": doc_content,
            "content_text": doc_content_text}
