from __future__ import annotations

import base64
import hashlib
from pathlib import Path


MIME_BY_MAGIC = (
    (b"\xff\xd8\xff", "image/jpeg"),
    (b"\x89PNG\r\n\x1a\n", "image/png"),
    (b"GIF87a", "image/gif"),
    (b"GIF89a", "image/gif"),
    (b"BM", "image/bmp"),
)


def iter_image_paths(input_dir: Path, recursive: bool):
    pattern = "**/*" if recursive else "*"
    yield from sorted(path for path in input_dir.glob(pattern) if path.is_file())


def detect_mime_type(image_bytes: bytes) -> str | None:
    for magic, mime_type in MIME_BY_MAGIC:
        if image_bytes.startswith(magic):
            return mime_type
    if image_bytes.startswith(b"RIFF") and image_bytes[8:12] == b"WEBP":
        return "image/webp"
    return None


def read_image_payload(path: Path) -> dict:
    image_bytes = path.read_bytes()
    mime_type = detect_mime_type(image_bytes)
    sha256 = hashlib.sha256(image_bytes).hexdigest()
    encoded = base64.b64encode(image_bytes).decode("ascii")
    return {
        "path": path,
        "sha256": sha256,
        "mime_type": mime_type,
        "size_bytes": len(image_bytes),
        "data_uri": f"data:{mime_type};base64,{encoded}" if mime_type else None,
    }
