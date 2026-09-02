import io

import pytest
from fastapi import UploadFile
from PIL import Image

from app.utilits.helpers import validate_image


def _png_upload(width: int, height: int, filename: str = "test.png") -> UploadFile:
    buf = io.BytesIO()
    Image.new("RGB", (width, height), color="red").save(buf, format="PNG")
    buf.seek(0)
    return UploadFile(file=buf, filename=filename)


def _svg_upload(content: bytes, filename: str = "test.svg") -> UploadFile:
    return UploadFile(file=io.BytesIO(content), filename=filename)


def test_accepts_a_valid_image_within_the_limits():
    upload = _png_upload(100, 100)
    validate_image(upload, max_size_bytes=1024 * 1024, min_width=16, min_height=16)
    # Must leave the file readable from the start for a subsequent save.
    assert upload.file.tell() == 0


def test_rejects_a_file_over_the_size_limit():
    upload = _png_upload(500, 500)
    actual_size = upload.file.seek(0, io.SEEK_END)
    upload.file.seek(0)

    with pytest.raises(ValueError, match="too large"):
        validate_image(upload, max_size_bytes=actual_size - 1, min_width=1, min_height=1)


def test_rejects_dimensions_below_the_minimum():
    upload = _png_upload(10, 10)

    with pytest.raises(ValueError, match="too small"):
        validate_image(upload, max_size_bytes=1024 * 1024, min_width=16, min_height=16)


def test_rejects_a_corrupted_or_non_image_file():
    upload = UploadFile(file=io.BytesIO(b"not actually an image"), filename="fake.png")

    with pytest.raises(ValueError, match="not a valid image"):
        validate_image(upload, max_size_bytes=1024 * 1024, min_width=1, min_height=1)


def test_accepts_a_genuine_svg_without_pillow_decoding_it():
    upload = _svg_upload(b'<?xml version="1.0"?><svg xmlns="http://www.w3.org/2000/svg"></svg>')
    validate_image(upload, max_size_bytes=1024 * 1024, min_width=16, min_height=16)


def test_rejects_a_file_with_svg_extension_that_is_not_actually_svg():
    upload = _svg_upload(b"not svg content at all")

    with pytest.raises(ValueError, match="not a valid SVG"):
        validate_image(upload, max_size_bytes=1024 * 1024, min_width=1, min_height=1)
