#!/usr/bin/env python3
"""
Compress images in static/gallery/ in-place using Pillow.

Usage:
    python3 compress_gallery.py [--max-width 1920] [--quality 82] [--dry-run]

Options:
    --max-width INT   Downscale images wider than this (default: 1920)
    --quality INT     JPEG/WebP quality 1-95 (default: 82)
    --dry-run         Print what would happen without saving anything

Only processes JPEG, PNG, and WebP files. Skips files already small enough.
GIFs are never touched.
"""

import argparse
import os
import sys

try:
    from PIL import Image
except ImportError:
    sys.exit("Pillow is required: pip install Pillow")

GALLERY_DIR = os.path.join(os.path.dirname(__file__), "static", "gallery")
SUPPORTED = {".jpg", ".jpeg", ".png", ".webp"}


def compress(path: str, max_width: int, quality: int, dry_run: bool) -> tuple[int, int]:
    """Return (original_bytes, new_bytes). new_bytes==original_bytes means skipped."""
    orig_size = os.path.getsize(path)
    ext = os.path.splitext(path)[1].lower()

    with Image.open(path) as img:
        w, h = img.size
        needs_resize = w > max_width
        new_w, new_h = (max_width, int(h * max_width / w)) if needs_resize else (w, h)

        if not needs_resize and orig_size < 150_000:
            return orig_size, orig_size

        if dry_run:
            action = f"resize {w}x{h}→{new_w}x{new_h}" if needs_resize else "compress only"
            print(f"  [dry-run] {os.path.basename(path)}: {action}, {orig_size//1024}KB")
            return orig_size, orig_size

        if needs_resize:
            img = img.resize((new_w, new_h), Image.LANCZOS)

        # Preserve orientation metadata
        exif = img.info.get("exif", b"")

        save_kwargs: dict = {"optimize": True}
        if ext in {".jpg", ".jpeg"}:
            save_kwargs["quality"] = quality
            save_kwargs["format"] = "JPEG"
            if exif:
                save_kwargs["exif"] = exif
        elif ext == ".webp":
            save_kwargs["quality"] = quality
            save_kwargs["format"] = "WebP"
        elif ext == ".png":
            save_kwargs["compress_level"] = 7
            save_kwargs["format"] = "PNG"

        img.save(path, **save_kwargs)

    new_size = os.path.getsize(path)
    return orig_size, new_size


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--max-width", type=int, default=1920, metavar="PX")
    parser.add_argument("--quality", type=int, default=82, metavar="1-95")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not os.path.isdir(GALLERY_DIR):
        sys.exit(f"Gallery directory not found: {GALLERY_DIR}")

    files = [
        f for f in sorted(os.listdir(GALLERY_DIR))
        if os.path.splitext(f)[1].lower() in SUPPORTED
    ]

    if not files:
        print("No supported images found.")
        return

    total_before = total_after = 0
    for fname in files:
        path = os.path.join(GALLERY_DIR, fname)
        before, after = compress(path, args.max_width, args.quality, args.dry_run)
        total_before += before
        total_after += after
        if not args.dry_run and after != before:
            saved = before - after
            pct = 100 * saved / before
            print(f"  {fname}: {before//1024}KB → {after//1024}KB  (-{saved//1024}KB, -{pct:.0f}%)")

    if not args.dry_run:
        saved_total = total_before - total_after
        pct_total = 100 * saved_total / total_before if total_before else 0
        print(f"\nDone. {len(files)} files: {total_before//1024}KB → {total_after//1024}KB  (-{saved_total//1024}KB, -{pct_total:.0f}%)")


if __name__ == "__main__":
    main()
