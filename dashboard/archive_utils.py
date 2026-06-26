"""
dashboard/archive_utils.py
==========================
Secure, streaming archive support for the Upload & Analyze page.

The dashboard's existing pipeline ingests a *directory* of flat ``.log`` / ``.txt``
files (both loaders glob the directory top-level only). This module lets an
operator upload a ``.zip`` / ``.tar`` / ``.tar.gz`` / ``.tgz`` archive instead:
the archive is extracted into a temporary directory, every supported log file is
discovered recursively, and those files are flattened into the batch staging
directory so the *unchanged* pipeline picks them up exactly as if they had been
uploaded one by one.

Design goals
------------
- **Security**   — every member is path-validated before extraction to defeat
                   Zip-Slip / Tar-Slip directory-traversal attacks. ``extractall``
                   is never used.
- **Streaming**  — members are copied out with ``shutil.copyfileobj`` so the full
                   archive is never materialised in memory; only supported log
                   files are extracted (unsupported entries are skipped, not
                   written to disk).
- **Single pass** — the extracted tree is walked exactly once to collect logs.
- **Cheap moves** — the temp directory is created on the *same filesystem* as the
                   batch directory, so flattening is a rename, not a copy.
- **No leaks**   — temp directories are always removed, even on exception.

Public API
----------
detect_archive_type(filename)            -> "zip" | "tar" | "tar.gz" | None
validate_archive_member(name, dest_root) -> resolved Path | None  (None = unsafe)
extract_archive(fileobj, kind, dest_dir) -> (extracted_count, rejected_count)
collect_log_files(root)                  -> list[Path]
cleanup_temp_directory(path)             -> None
stage_uploads(uploaded_files, batch_dir) -> StagingResult
"""

from __future__ import annotations

import os
import shutil
import tarfile
import tempfile
import time
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO, Iterable

from common.logger import get_logger

logger = get_logger(__name__)

# Log files the pipeline can ingest. Mirrors the loaders' globs
# (parsing/sessionizer.run_directory accepts .log/.txt; the synthetic loader
# globs *.log). We collect both and let the resolved parsing mode decide.
SUPPORTED_LOG_EXTS: frozenset[str] = frozenset({".log", ".txt"})

# Stream copy buffer — 1 MiB keeps memory flat regardless of file size.
_COPY_BUF = 1024 * 1024


# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------
@dataclass
class StagingResult:
    """Outcome of staging a mixed batch of plain files and/or archives."""

    staged_files: list[Path] = field(default_factory=list)
    archive_count: int = 0
    extracted_log_count: int = 0
    rejected_unsafe_count: int = 0
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    elapsed_seconds: float = 0.0


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------
def detect_archive_type(filename: str) -> str | None:
    """Return the archive kind for *filename*, or ``None`` if it is not an archive.

    Detection is by extension (the uploaded name); content sniffing is left to
    the extractor, which opens tar streams with auto-detection (``r:*``).
    """
    name = filename.lower()
    if name.endswith(".zip"):
        return "zip"
    if name.endswith((".tar.gz", ".tgz")):
        return "tar.gz"
    if name.endswith(".tar"):
        return "tar"
    return None


# ---------------------------------------------------------------------------
# Security — path validation (Zip-Slip / Tar-Slip guard)
# ---------------------------------------------------------------------------
def validate_archive_member(member_name: str, dest_root: Path) -> Path | None:
    """Resolve *member_name* under *dest_root* and verify it cannot escape.

    Returns the safe, resolved destination ``Path`` if the member stays inside
    *dest_root*; returns ``None`` for any traversal attempt (``../``), absolute
    path, or drive-letter path (``C:\\Windows\\...``). ``Path`` join semantics
    reset to the absolute target for absolute members, so the subsequent
    ``relative_to`` containment check rejects them.
    """
    if not member_name or member_name in (".", ".."):
        return None

    root = dest_root.resolve()
    # Normalise both separators so Windows-style entries inside a zip created on
    # another OS are handled consistently.
    normalised = member_name.replace("\\", "/")
    target = (root / normalised).resolve()

    try:
        target.relative_to(root)
    except ValueError:
        return None
    return target


# ---------------------------------------------------------------------------
# Extraction — streaming, supported files only
# ---------------------------------------------------------------------------
def _safe_stream_to_disk(src: BinaryIO, target: Path) -> None:
    """Stream *src* to *target*, creating parent dirs. Never buffers whole file."""
    target.parent.mkdir(parents=True, exist_ok=True)
    with open(target, "wb") as dst:
        shutil.copyfileobj(src, dst, length=_COPY_BUF)


def _extract_zip(fileobj: BinaryIO, dest_root: Path) -> tuple[int, int]:
    extracted = rejected = 0
    with zipfile.ZipFile(fileobj) as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            if Path(info.filename).suffix.lower() not in SUPPORTED_LOG_EXTS:
                continue  # ignore unsupported entries — never written to disk
            target = validate_archive_member(info.filename, dest_root)
            if target is None:
                logger.warning("Rejected unsafe zip entry: %s", info.filename)
                rejected += 1
                continue
            with zf.open(info) as src:  # raises RuntimeError if encrypted
                _safe_stream_to_disk(src, target)
            extracted += 1
    return extracted, rejected


def _extract_tar(fileobj: BinaryIO, dest_root: Path) -> tuple[int, int]:
    extracted = rejected = 0
    # "r:*" transparently handles tar, tar.gz and tgz. Streaming iteration keeps
    # only one member's metadata in memory at a time.
    with tarfile.open(fileobj=fileobj, mode="r:*") as tf:
        for member in tf:
            # Only regular files are candidates; symlinks/hardlinks/devices/dirs
            # are skipped outright (a symlink's target could escape dest_root).
            if not member.isfile():
                continue
            if Path(member.name).suffix.lower() not in SUPPORTED_LOG_EXTS:
                continue
            target = validate_archive_member(member.name, dest_root)
            if target is None:
                logger.warning("Rejected unsafe tar entry: %s", member.name)
                rejected += 1
                continue
            src = tf.extractfile(member)
            if src is None:
                continue
            with src:
                _safe_stream_to_disk(src, target)
            extracted += 1
    return extracted, rejected


def extract_archive(fileobj: BinaryIO, kind: str, dest_dir: Path) -> tuple[int, int]:
    """Extract supported log files from *fileobj* into *dest_dir*.

    Args:
        fileobj:  A seekable binary stream (e.g. a Streamlit ``UploadedFile``).
        kind:     One of ``"zip" | "tar" | "tar.gz"`` (from ``detect_archive_type``).
        dest_dir: Temporary extraction root (must already exist).

    Returns:
        ``(extracted_count, rejected_unsafe_count)``.

    Raises:
        zipfile.BadZipFile / tarfile.TarError on corrupt archives;
        RuntimeError for password-protected zips. Callers handle these.
    """
    try:
        fileobj.seek(0)
    except Exception:
        pass

    if kind == "zip":
        return _extract_zip(fileobj, dest_dir)
    if kind in ("tar", "tar.gz"):
        return _extract_tar(fileobj, dest_dir)
    raise ValueError(f"Unknown archive kind: {kind!r}")


# ---------------------------------------------------------------------------
# Discovery — single traversal
# ---------------------------------------------------------------------------
def collect_log_files(root: Path) -> list[Path]:
    """Recursively collect supported log files under *root* in one ``os.walk``."""
    found: list[Path] = []
    for dirpath, _dirs, filenames in os.walk(root):
        for fn in filenames:
            if Path(fn).suffix.lower() in SUPPORTED_LOG_EXTS:
                found.append(Path(dirpath) / fn)
    return found


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
def cleanup_temp_directory(path: Path) -> None:
    """Remove *path* recursively; never raises (best-effort, idempotent)."""
    shutil.rmtree(path, ignore_errors=True)


# ---------------------------------------------------------------------------
# Flatten extracted files into the (flat) batch directory
# ---------------------------------------------------------------------------
def _unique_name(name: str, used: set[str]) -> str:
    """Return a filename not already in *used*, appending ``_N`` before the suffix."""
    if name not in used:
        return name
    stem, dot, suffix = name.partition(".")
    n = 1
    while True:
        candidate = f"{stem}_{n}{dot}{suffix}"
        if candidate not in used:
            return candidate
        n += 1


def _flatten_into(files: Iterable[Path], src_root: Path, dest_dir: Path,
                  used: set[str]) -> list[Path]:
    """Move *files* into *dest_dir*, encoding their relative path into the name.

    ``logs/app1/app.log`` -> ``logs__app1__app.log`` so files with the same base
    name in different sub-directories never collide. The move is a same-filesystem
    rename when *src_root* and *dest_dir* share a volume (see ``stage_uploads``).
    """
    moved: list[Path] = []
    for f in files:
        rel = f.relative_to(src_root)
        flat = _unique_name("__".join(rel.parts), used)
        used.add(flat)
        dest = dest_dir / flat
        shutil.move(str(f), str(dest))
        moved.append(dest)
    return moved


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
def _process_one_archive(uf, kind: str, batch_dir: Path, result: StagingResult,
                         used_names: set[str]) -> None:
    """Extract a single uploaded archive into *batch_dir*, updating *result*."""
    t0 = time.perf_counter()
    logger.info("Archive detected: %s (kind=%s)", uf.name, kind)

    # Temp dir lives beside batch_dir → same filesystem → flatten is a rename.
    tmp = Path(tempfile.mkdtemp(prefix="extract_", dir=str(batch_dir.parent)))
    try:
        logger.info("Extraction started: %s -> %s", uf.name, tmp)
        extracted, rejected = extract_archive(uf, kind, tmp)
        result.rejected_unsafe_count += rejected

        logs = collect_log_files(tmp)
        logger.info(
            "Extraction completed: %s — %d file(s) extracted, %d valid log(s), "
            "%d unsafe entr(ies) rejected",
            uf.name, extracted, len(logs), rejected,
        )

        if not logs:
            result.warnings.append(
                f"'{uf.name}': no supported .log/.txt files found in archive."
            )
            return

        moved = _flatten_into(logs, tmp, batch_dir, used_names)
        result.staged_files.extend(moved)
        result.extracted_log_count += len(moved)
        result.archive_count += 1

    except (zipfile.BadZipFile, tarfile.ReadError):
        result.errors.append(f"'{uf.name}': corrupted or unreadable archive.")
        logger.warning("Corrupted archive: %s", uf.name)
    except tarfile.TarError as exc:
        result.errors.append(f"'{uf.name}': tar extraction failed ({exc}).")
        logger.warning("Tar error for %s: %s", uf.name, exc)
    except RuntimeError as exc:
        # zipfile raises RuntimeError for encrypted entries ("password required").
        if "password" in str(exc).lower() or "encrypt" in str(exc).lower():
            result.errors.append(
                f"'{uf.name}': password-protected archives are not supported."
            )
            logger.warning("Password-protected archive: %s", uf.name)
        else:
            result.errors.append(f"'{uf.name}': extraction failed ({exc}).")
            logger.warning("Extraction RuntimeError for %s: %s", uf.name, exc)
    except OSError as exc:
        # ENOSPC (disk full), permission, and other I/O failures.
        if getattr(exc, "errno", None) == 28:  # errno.ENOSPC
            result.errors.append(
                f"'{uf.name}': not enough disk space to extract archive."
            )
        elif isinstance(exc, PermissionError):
            result.errors.append(f"'{uf.name}': permission denied during extraction.")
        else:
            result.errors.append(f"'{uf.name}': I/O error during extraction ({exc}).")
        logger.warning("OSError extracting %s: %s", uf.name, exc)
    finally:
        cleanup_temp_directory(tmp)
        logger.info(
            "Cleanup completed for %s (%.2fs)", uf.name, time.perf_counter() - t0
        )


def stage_uploads(uploaded_files, batch_dir: Path) -> StagingResult:
    """Stage a mixed batch of plain log files and archives into *batch_dir*.

    Plain ``.log`` / ``.txt`` files are written straight into the (flat) batch
    directory. Archives are extracted securely and their log files flattened in
    alongside. The resulting directory is exactly what the existing pipeline
    expects — no pipeline change is required.

    Returns a :class:`StagingResult` carrying the staged paths plus user-facing
    warnings / errors and summary stats.
    """
    t0 = time.perf_counter()
    result = StagingResult()
    batch_dir.mkdir(parents=True, exist_ok=True)
    used_names: set[str] = set()

    for uf in uploaded_files:
        kind = detect_archive_type(uf.name)
        if kind is None:
            # Plain file — preserve existing behaviour.
            if Path(uf.name).suffix.lower() not in SUPPORTED_LOG_EXTS:
                result.warnings.append(f"'{uf.name}': unsupported file type, skipped.")
                continue
            flat = _unique_name(uf.name, used_names)
            used_names.add(flat)
            dest = batch_dir / flat
            try:
                dest.write_bytes(uf.getvalue())
                result.staged_files.append(dest)
            except OSError as exc:
                result.errors.append(f"'{uf.name}': could not stage file ({exc}).")
                logger.warning("Failed to stage plain file %s: %s", uf.name, exc)
        else:
            _process_one_archive(uf, kind, batch_dir, result, used_names)

    result.elapsed_seconds = time.perf_counter() - t0
    logger.info(
        "Staging done: %d log file(s) staged from %d archive(s) + plain uploads "
        "in %.2fs",
        len(result.staged_files), result.archive_count, result.elapsed_seconds,
    )
    return result
