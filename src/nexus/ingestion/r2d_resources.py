"""Cross-platform process-tree resource evidence for the portable R2D-A1 verifier.

The monitor is observational. It never signals, terminates, or otherwise manages a
descendant process, and any measurement failure makes the evidence fail closed.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Self

import psutil

from nexus.ingestion.r2d_contracts import R2DConfig, R2DResourceContract

MIB = 1024 * 1024

WALL_TIME_METHOD = (
    "time.perf_counter monotonic elapsed from context entry through bounded descendant "
    "observation and artifact measurement"
)
RSS_METHOD = (
    "periodic psutil samples summing memory_info().rss for the root process and all "
    "recursive descendants; maximum sampled byte sum"
)
NATIVE_THREAD_METHOD = (
    "periodic psutil samples summing num_threads() for the root process and all recursive "
    "descendants; maximum sampled native-thread sum"
)
ARTIFACT_METHOD = (
    "post-scope os.walk with followlinks=False; sum st_size for each unique regular file "
    "under governed roots; symbolic links and non-regular entries are refused"
)
SHUTDOWN_METHOD = (
    "time.perf_counter elapsed while polling tracked pid/create_time identities and current "
    "recursive descendants up to the configured limit; no signals or termination"
)
INTEGRITY_METHOD = (
    "timer, sampler-thread, psutil process-tree, and governed-filesystem measurements all "
    "completed without an error"
)


@dataclass(frozen=True, slots=True)
class R2DResourceCheck:
    """One independently auditable resource assertion."""

    name: str
    passed: bool
    observed: object
    limit: object
    comparator: str
    measurement_method: str

    def to_dict(self) -> dict[str, object]:
        """Return a stable JSON-ready representation."""

        return {
            "name": self.name,
            "status": "PASS" if self.passed else "FAIL",
            "passed": self.passed,
            "observed": self.observed,
            "limit": self.limit,
            "comparator": self.comparator,
            "measurement_method": self.measurement_method,
        }


@dataclass(frozen=True, slots=True)
class R2DResourceEvidence:
    """Complete measured resource envelope for one portable verification run."""

    wall_time_seconds: float
    peak_process_tree_rss_bytes: int
    peak_process_tree_native_threads: int
    maximum_descendant_count: int
    shutdown_time_seconds: float
    surviving_descendant_pids: tuple[int, ...]
    governed_artifact_bytes: int
    governed_artifact_file_count: int
    sample_interval_seconds: float
    budget: R2DResourceContract
    checks: tuple[R2DResourceCheck, ...]
    measurement_errors: tuple[str, ...]

    @property
    def passed(self) -> bool:
        """Whether every budget and measurement-integrity assertion passed."""

        return (
            bool(self.checks)
            and not self.measurement_errors
            and all(check.passed for check in self.checks)
        )

    @property
    def peak_process_tree_rss_mib(self) -> float:
        """Expose the binary-megabyte high water without losing byte precision."""

        return self.peak_process_tree_rss_bytes / MIB

    def to_dict(self) -> dict[str, object]:
        """Return exact, JSON-ready evidence without hiding raw timing observations."""

        return {
            "status": "PASS" if self.passed else "FAIL",
            "passed": self.passed,
            "measurement": {
                "wall_time": WALL_TIME_METHOD,
                "peak_process_tree_rss": RSS_METHOD,
                "peak_process_tree_native_threads": NATIVE_THREAD_METHOD,
                "shutdown": SHUTDOWN_METHOD,
                "governed_artifacts": ARTIFACT_METHOD,
                "integrity": INTEGRITY_METHOD,
                "sample_interval_seconds": self.sample_interval_seconds,
            },
            "budget": self.budget.model_dump(mode="json"),
            "observed": {
                "wall_time_seconds": self.wall_time_seconds,
                "peak_process_tree_rss_bytes": self.peak_process_tree_rss_bytes,
                "peak_process_tree_rss_mib": self.peak_process_tree_rss_mib,
                "peak_process_tree_native_threads": self.peak_process_tree_native_threads,
                "maximum_descendant_count": self.maximum_descendant_count,
                "shutdown_time_seconds": self.shutdown_time_seconds,
                "surviving_descendant_pids": list(self.surviving_descendant_pids),
                "governed_artifact_bytes": self.governed_artifact_bytes,
                "governed_artifact_file_count": self.governed_artifact_file_count,
            },
            "checks": [check.to_dict() for check in self.checks],
            "measurement_errors": list(self.measurement_errors),
        }

    @property
    def semantic_sha256(self) -> str:
        """Hash the complete exact evidence; cross-run timing is intentionally not normalized."""

        encoded = json.dumps(
            self.to_dict(),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True, slots=True)
class _ArtifactUsage:
    bytes: int
    file_count: int
    errors: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _ProcessIdentity:
    pid: int
    create_time: float


@dataclass(frozen=True, slots=True)
class _ProcessSample:
    rss_bytes: int
    native_threads: int
    descendant_count: int
    identities: tuple[_ProcessIdentity, ...]
    errors: tuple[str, ...]


def _measure_governed_artifacts(roots: tuple[Path, ...]) -> _ArtifactUsage:
    """Measure unique regular files without following any symbolic link."""

    total_bytes = 0
    file_count = 0
    measured_paths: set[str] = set()
    errors: list[str] = []

    for supplied_root in roots:
        root = supplied_root.absolute()
        try:
            try:
                root_stat = root.lstat()
            except FileNotFoundError:
                continue
            if stat.S_ISLNK(root_stat.st_mode):
                errors.append(f"artifact root is a symbolic link: {root}")
                continue
            candidates: tuple[Path, ...]
            if stat.S_ISREG(root_stat.st_mode):
                candidates = (root,)
            elif stat.S_ISDIR(root_stat.st_mode):
                walked: list[Path] = []

                def record_walk_error(error: OSError) -> None:
                    errors.append(f"artifact traversal failed: {type(error).__name__}: {error}")

                for directory, directory_names, file_names in os.walk(
                    root,
                    topdown=True,
                    onerror=record_walk_error,
                    followlinks=False,
                ):
                    directory_names.sort()
                    file_names.sort()
                    base = Path(directory)
                    retained_directories: list[str] = []
                    for name in directory_names:
                        candidate = base / name
                        try:
                            candidate_mode = candidate.lstat().st_mode
                        except OSError as error:
                            errors.append(
                                f"artifact directory measurement failed for {candidate}: "
                                f"{type(error).__name__}: {error}"
                            )
                            continue
                        if stat.S_ISLNK(candidate_mode):
                            errors.append(f"symbolic-link artifact directory refused: {candidate}")
                        elif not stat.S_ISDIR(candidate_mode):
                            errors.append(f"non-directory traversal entry refused: {candidate}")
                        else:
                            retained_directories.append(name)
                    directory_names[:] = retained_directories
                    walked.extend(base / name for name in file_names)
                candidates = tuple(walked)
            else:
                errors.append(f"artifact root is not a regular file or directory: {root}")
                continue

            for candidate in candidates:
                try:
                    candidate_stat = candidate.lstat()
                    if stat.S_ISLNK(candidate_stat.st_mode):
                        errors.append(f"symbolic-link artifact file refused: {candidate}")
                        continue
                    if not stat.S_ISREG(candidate_stat.st_mode):
                        errors.append(f"non-regular artifact entry refused: {candidate}")
                        continue
                    path_key = os.path.normcase(str(candidate.resolve(strict=True)))
                    if path_key in measured_paths:
                        continue
                    measured_paths.add(path_key)
                    total_bytes += candidate_stat.st_size
                    file_count += 1
                except OSError as error:
                    errors.append(
                        f"artifact file measurement failed for {candidate}: "
                        f"{type(error).__name__}: {error}"
                    )
        except OSError as error:
            errors.append(
                f"artifact root measurement failed for {root}: {type(error).__name__}: {error}"
            )

    return _ArtifactUsage(
        bytes=total_bytes,
        file_count=file_count,
        errors=tuple(sorted(set(errors))),
    )


def _sample_process_tree(root: psutil.Process) -> _ProcessSample:
    """Collect one best-effort process-tree sample and retain all integrity errors."""

    errors: list[str] = []
    try:
        descendants = root.children(recursive=True)
    except (psutil.AccessDenied, psutil.NoSuchProcess) as error:
        return _ProcessSample(
            rss_bytes=0,
            native_threads=0,
            descendant_count=0,
            identities=(),
            errors=(f"process-tree enumeration failed: {type(error).__name__}: {error}",),
        )

    rss_bytes = 0
    native_threads = 0
    identities: list[_ProcessIdentity] = []
    for process in (root, *descendants):
        try:
            with process.oneshot():
                rss_bytes += process.memory_info().rss
                native_threads += process.num_threads()
                if process.pid != root.pid:
                    identities.append(
                        _ProcessIdentity(pid=process.pid, create_time=process.create_time())
                    )
        except (psutil.NoSuchProcess, psutil.ZombieProcess):
            if process.pid == root.pid:
                errors.append("root process disappeared during resource measurement")
        except psutil.AccessDenied as error:
            errors.append(
                f"access denied measuring process {process.pid}: {type(error).__name__}: {error}"
            )
    return _ProcessSample(
        rss_bytes=rss_bytes,
        native_threads=native_threads,
        descendant_count=len(descendants),
        identities=tuple(identities),
        errors=tuple(errors),
    )


class R2DResourceMonitor:
    """Observe the complete portable verifier process tree and governed artifacts."""

    def __init__(
        self,
        config: R2DConfig,
        *,
        governed_artifact_roots: tuple[Path, ...],
        sample_interval_seconds: float = 0.02,
    ) -> None:
        if not 0.001 <= sample_interval_seconds <= 1.0:
            raise ValueError("sample_interval_seconds must be between 0.001 and 1.0")
        if not governed_artifact_roots:
            raise ValueError("at least one governed artifact root is required")

        self._config = config
        self._artifact_roots = tuple(Path(root) for root in governed_artifact_roots)
        self._sample_interval_seconds = sample_interval_seconds
        self._root = psutil.Process()
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._sampler_thread: threading.Thread | None = None
        self._started_at: float | None = None
        self._evidence: R2DResourceEvidence | None = None
        self._measurement_errors: set[str] = set()
        self._descendants_seen: dict[int, _ProcessIdentity] = {}
        self._baseline_descendants: set[_ProcessIdentity] = set()
        self._peak_rss_bytes = 0
        self._peak_native_threads = 0
        self._maximum_descendant_count = 0

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: object,
    ) -> Literal[False]:
        del exception_type, exception, traceback
        self.stop()
        return False

    @property
    def evidence(self) -> R2DResourceEvidence:
        """Return finalized evidence; accessing it while running is an error."""

        if self._evidence is None:
            raise RuntimeError("resource evidence is available only after the monitor stops")
        return self._evidence

    def start(self) -> None:
        """Start the bounded background sampler exactly once."""

        if self._started_at is not None:
            raise RuntimeError("R2D resource monitor cannot be started more than once")
        # A verifier embedded in a larger test process must not claim that
        # children already present at entry are its own shutdown leaks.
        try:
            for process in self._root.children(recursive=True):
                try:
                    self._baseline_descendants.add(
                        _ProcessIdentity(process.pid, process.create_time())
                    )
                except (psutil.NoSuchProcess, psutil.ZombieProcess):
                    continue
                except psutil.AccessDenied as error:
                    self._record_error(f"baseline process identity unavailable: {error}")
        except (psutil.AccessDenied, psutil.NoSuchProcess) as error:
            self._record_error(f"baseline process-tree enumeration failed: {error}")
        self._started_at = time.perf_counter()
        self._sample_once()
        self._sampler_thread = threading.Thread(
            target=self._sample_until_stopped,
            name="r2d-process-tree-resource-sampler",
            daemon=True,
        )
        self._sampler_thread.start()

    def stop(self) -> R2DResourceEvidence:
        """Stop sampling, observe cleanup, measure artifacts, and evaluate limits."""

        if self._evidence is not None:
            return self._evidence
        if self._started_at is None:
            raise RuntimeError("R2D resource monitor must be started before it can stop")

        self._stop_event.set()
        sampler_thread = self._sampler_thread
        if sampler_thread is not None:
            sampler_thread.join(timeout=max(1.0, self._sample_interval_seconds * 5))
            if sampler_thread.is_alive():
                self._record_error("sampler thread did not stop within its bounded join timeout")
        self._sample_once()

        shutdown_started = time.perf_counter()
        survivors = self._observe_descendant_cleanup()
        shutdown_time_seconds = time.perf_counter() - shutdown_started
        artifact_usage = _measure_governed_artifacts(self._artifact_roots)
        for error in artifact_usage.errors:
            self._record_error(error)
        wall_time_seconds = time.perf_counter() - self._started_at

        with self._lock:
            measurement_errors = tuple(sorted(self._measurement_errors))
            peak_rss_bytes = self._peak_rss_bytes
            peak_native_threads = self._peak_native_threads
            maximum_descendant_count = self._maximum_descendant_count

        self._evidence = self._evaluate(
            wall_time_seconds=wall_time_seconds,
            peak_rss_bytes=peak_rss_bytes,
            peak_native_threads=peak_native_threads,
            maximum_descendant_count=maximum_descendant_count,
            shutdown_time_seconds=shutdown_time_seconds,
            survivors=survivors,
            artifact_usage=artifact_usage,
            measurement_errors=measurement_errors,
        )
        return self._evidence

    def _sample_until_stopped(self) -> None:
        try:
            while not self._stop_event.wait(self._sample_interval_seconds):
                self._sample_once()
        except Exception as error:  # pragma: no cover - defensive thread boundary
            self._record_error(f"sampler thread failed: {type(error).__name__}: {error}")

    def _sample_once(self) -> None:
        try:
            sample = _sample_process_tree(self._root)
        except Exception as error:  # pragma: no cover - injected/defensive boundary
            self._record_error(f"process-tree sampler failed: {type(error).__name__}: {error}")
            return
        with self._lock:
            self._peak_rss_bytes = max(self._peak_rss_bytes, sample.rss_bytes)
            self._peak_native_threads = max(
                self._peak_native_threads,
                sample.native_threads,
            )
            self._maximum_descendant_count = max(
                self._maximum_descendant_count,
                sample.descendant_count,
            )
            self._descendants_seen.update(
                {identity.pid: identity for identity in sample.identities}
            )
            self._measurement_errors.update(sample.errors)

    def _observe_descendant_cleanup(self) -> tuple[int, ...]:
        deadline = time.perf_counter() + self._config.resource_contract.shutdown_seconds
        survivors: tuple[int, ...] = ()
        while True:
            survivors, complete = self._surviving_descendants_once()
            if not complete or not survivors:
                return survivors
            remaining = deadline - time.perf_counter()
            if remaining <= 0:
                return survivors
            time.sleep(min(self._sample_interval_seconds, remaining))

    def _surviving_descendants_once(self) -> tuple[tuple[int, ...], bool]:
        try:
            current_descendants = self._root.children(recursive=True)
        except (psutil.AccessDenied, psutil.NoSuchProcess) as error:
            self._record_error(
                f"cleanup process-tree enumeration failed: {type(error).__name__}: {error}"
            )
            return (), False

        with self._lock:
            identities = dict(self._descendants_seen)
        for process in current_descendants:
            try:
                identities[process.pid] = _ProcessIdentity(
                    pid=process.pid,
                    create_time=process.create_time(),
                )
            except (psutil.NoSuchProcess, psutil.ZombieProcess):
                continue
            except psutil.AccessDenied as error:
                self._record_error(
                    f"access denied identifying cleanup process {process.pid}: "
                    f"{type(error).__name__}: {error}"
                )
                return tuple(sorted(identities)), False

        survivors: list[int] = []
        for identity in identities.values():
            if identity in self._baseline_descendants:
                continue
            try:
                process = psutil.Process(identity.pid)
                if process.create_time() == identity.create_time and process.is_running():
                    survivors.append(identity.pid)
            except (psutil.NoSuchProcess, psutil.ZombieProcess):
                continue
            except psutil.AccessDenied as error:
                self._record_error(
                    f"access denied checking cleanup process {identity.pid}: "
                    f"{type(error).__name__}: {error}"
                )
                survivors.append(identity.pid)
                return tuple(sorted(set(survivors))), False
        return tuple(sorted(set(survivors))), True

    def _record_error(self, message: str) -> None:
        with self._lock:
            self._measurement_errors.add(message)

    def _evaluate(
        self,
        *,
        wall_time_seconds: float,
        peak_rss_bytes: int,
        peak_native_threads: int,
        maximum_descendant_count: int,
        shutdown_time_seconds: float,
        survivors: tuple[int, ...],
        artifact_usage: _ArtifactUsage,
        measurement_errors: tuple[str, ...],
    ) -> R2DResourceEvidence:
        budget = self._config.resource_contract
        rss_limit_bytes = budget.peak_process_tree_rss_mib * MIB
        artifact_limit_bytes = budget.artifact_mib * MIB
        integrity_passed = not measurement_errors
        checks = (
            R2DResourceCheck(
                "measurement_integrity",
                integrity_passed,
                len(measurement_errors),
                0,
                "==",
                INTEGRITY_METHOD,
            ),
            R2DResourceCheck(
                "wall_time_seconds",
                wall_time_seconds <= budget.wall_time_seconds,
                wall_time_seconds,
                budget.wall_time_seconds,
                "<=",
                WALL_TIME_METHOD,
            ),
            R2DResourceCheck(
                "peak_process_tree_rss_bytes",
                peak_rss_bytes <= rss_limit_bytes,
                peak_rss_bytes,
                rss_limit_bytes,
                "<=",
                RSS_METHOD,
            ),
            R2DResourceCheck(
                "peak_process_tree_native_threads",
                peak_native_threads <= budget.maximum_process_tree_threads,
                peak_native_threads,
                budget.maximum_process_tree_threads,
                "<=",
                NATIVE_THREAD_METHOD,
            ),
            R2DResourceCheck(
                "governed_artifact_bytes",
                not artifact_usage.errors and artifact_usage.bytes <= artifact_limit_bytes,
                artifact_usage.bytes,
                artifact_limit_bytes,
                "<=",
                ARTIFACT_METHOD,
            ),
            R2DResourceCheck(
                "shutdown_time_seconds",
                shutdown_time_seconds <= budget.shutdown_seconds,
                shutdown_time_seconds,
                budget.shutdown_seconds,
                "<=",
                SHUTDOWN_METHOD,
            ),
            R2DResourceCheck(
                "surviving_descendant_count",
                integrity_passed and len(survivors) <= budget.maximum_surviving_descendants,
                len(survivors),
                budget.maximum_surviving_descendants,
                "<=",
                SHUTDOWN_METHOD,
            ),
        )
        return R2DResourceEvidence(
            wall_time_seconds=wall_time_seconds,
            peak_process_tree_rss_bytes=peak_rss_bytes,
            peak_process_tree_native_threads=peak_native_threads,
            maximum_descendant_count=maximum_descendant_count,
            shutdown_time_seconds=shutdown_time_seconds,
            surviving_descendant_pids=survivors,
            governed_artifact_bytes=artifact_usage.bytes,
            governed_artifact_file_count=artifact_usage.file_count,
            sample_interval_seconds=self._sample_interval_seconds,
            budget=budget,
            checks=checks,
            measurement_errors=measurement_errors,
        )
