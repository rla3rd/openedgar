import pyzstd

c = pyzstd.ZstdCompressor(3)
chunk1 = b"hello "
chunk2 = b"world"
out1 = c.compress(chunk1)
out2 = c.compress(chunk2)
out3 = c.flush()

full = out1 + out2 + out3
print("Total output length:", len(full))
try:
    decompressed = pyzstd.decompress(full)
    print("Decompressed:", decompressed)
except Exception as e:
    print("Decompression error:", e)
