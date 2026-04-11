"""
MIT License

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
import logging
import os
import zstandard as zstd
from pathlib import Path

# Setup logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logger.addHandler(console)


class LocalClient:

    def __init__(self):
        logger.info("Initialized local client")

    def path_exists(self, path: str):
        return os.path.exists(path)

    def put_buffer(self, file_path: str, buffer, write_bytes=True):
        dir_name = os.path.dirname(file_path)
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
        
        if file_path.endswith('.zst'):
            cctx = zstd.ZstdCompressor(level=3)
            # Ensure buffer is bytes
            if not isinstance(buffer, bytes):
                buffer = buffer.encode('utf-8')
            with open(file_path, 'wb') as f:
                f.write(cctx.compress(buffer))
        else:
            mode = "wb" if write_bytes else "w"
            with open(file_path, mode=mode) as localfile:
                localfile.write(buffer)

    def get_buffer(self, file_path: str):
        # 1. Try compressed .zst version first
        zst_path = file_path if file_path.endswith('.zst') else f"{file_path}.zst"
        if os.path.exists(zst_path):
            dctx = zstd.ZstdDecompressor()
            with open(zst_path, 'rb') as f:
                return dctx.decompress(f.read())
        
        # 2. Fallback to uncompressed plain file
        if os.path.exists(file_path):
            with open(file_path, mode='rb') as localfile:
                return localfile.read()
        
        raise FileNotFoundError(f"Could not find filing at {file_path} or {file_path}.zst")
