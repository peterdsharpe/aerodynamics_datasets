#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "requests",
#     "tqdm",
# ]
# ///
"""Downloader for the DrivAerML dataset hosted on Hugging Face.

Simple, readable script inspired by airfrans/download.py. Edit the constants
below to configure the owner/prefix, local directory, run IDs, and per-run file
patterns. For patterns that don't exist directly, the script will attempt
`.00.part`, `.01.part`, ... and assemble them into the final file.

Run with:
    uv run drivaerml/download.py
"""

from typing import Sequence
from itertools import count
from pathlib import Path
import sys

import requests
from tqdm import tqdm


### [Config]
HF_OWNER = "neashton"
HF_PREFIX = "drivaerml"
LOCAL_DIR = Path("./drivaer_data")
RUN_IDS: Sequence[int] = [1]

# Edit this list to add/remove per-run files. Use %d for the run index.
FILE_PATTERNS: list[str] = [
    "drivaer_%d.stl",
    "force_mom_%d.csv",
    "volume_%d.vtu",
]

BASE_URL = (
    "https://huggingface.co/datasets/{owner}/{prefix}/resolve/main/{run_dir}/{fname}"
)


def _download_stream(url: str, dest: Path | str, description: str) -> bool:
    """Stream a URL to a file with a progress bar.

    Returns True if downloaded; returns False if the server responds 404.
    Raises for other HTTP errors.
    """
    dest_path = Path(dest)
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    # HEAD to detect 404 early and get size when available
    try:
        head = requests.head(url, allow_redirects=True, timeout=30)
    except Exception as exc:  # fall back to GET without size on HEAD failure
        print(f"HEAD failed for {url!r}: {exc}", file=sys.stderr)
        head = None

    if head is not None and head.status_code == 404:
        return False

    total_size = 0
    if head is not None and head.status_code == 200:
        try:
            total_size = int(head.headers.get("content-length", "0"))
        except ValueError:
            total_size = 0

    with requests.get(url, stream=True) as resp:
        if resp.status_code == 404:
            return False
        resp.raise_for_status()

        with (
            open(dest_path, "wb") as fh,
            tqdm(
                total=total_size,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc=description,
            ) as pbar,
        ):
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    fh.write(chunk)
                    if total_size:
                        pbar.update(len(chunk))

    return True


def download_direct_or_parts(run_dir: str, fname: str, dest: Path | str) -> None:
    """Download a file directly, falling back to `.NN.part` fragments.

    If fragments were downloaded, they are concatenated into the final file.
    """
    dest_path = Path(dest)
    if dest_path.exists():
        print(f"{dest_path} exists, skipping.")
        return

    url = BASE_URL.format(
        owner=HF_OWNER, prefix=HF_PREFIX, run_dir=run_dir, fname=fname
    )
    if _download_stream(url, dest_path, description=f"{run_dir}/{fname}"):
        return

    # Attempt multi-part fallback
    part_paths: list[Path] = []
    for part_idx in count(0):
        part_suffix = f".{part_idx:02d}.part"
        part_fname = f"{fname}{part_suffix}"
        part_url = BASE_URL.format(
            owner=HF_OWNER, prefix=HF_PREFIX, run_dir=run_dir, fname=part_fname
        )
        part_path = dest_path.parent / part_fname

        ok = _download_stream(
            part_url, part_path, description=f"{run_dir}/{part_fname}"
        )
        if not ok:
            break
        part_paths.append(part_path)

    if not part_paths:
        print(f"No remote file or parts found for {dest_path.name}", file=sys.stderr)
        return

    print(f"Assembling {dest_path.name} from {len(part_paths)} parts…")
    with open(dest_path, "wb") as dst:
        for part_path in sorted(part_paths):
            with open(part_path, "rb") as src:
                while True:
                    chunk = src.read(64 * 1024)
                    if not chunk:
                        break
                    dst.write(chunk)

    for p in part_paths:
        p.unlink(missing_ok=True)


def process_run(run_idx: int) -> None:
    """Download all configured files for a single run index."""
    run_dir = f"run_{run_idx}"
    run_local_dir = LOCAL_DIR / run_dir
    run_local_dir.mkdir(parents=True, exist_ok=True)

    for pattern in FILE_PATTERNS:
        target_name = pattern % run_idx
        target_path = run_local_dir / target_name
        download_direct_or_parts(run_dir=run_dir, fname=target_name, dest=target_path)


if __name__ == "__main__":
    print("Downloading DrivAerML dataset…")
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)

    for i in RUN_IDS:
        process_run(i)

    print("Download complete!")
