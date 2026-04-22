import pyzstd
import zlib

try:
    c = pyzstd.ZstdCompressor(3)
    print("ZstdCompressor created")
    print("Has compress:", hasattr(c, "compress"))
    print("Has flush:", hasattr(c, "flush"))
except Exception as e:
    print("Error:", e)
