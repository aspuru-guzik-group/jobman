from pathlib import Path
import shutil

from .base_job_source import BaseJobSource


class DirJobSource(BaseJobSource):
    DEFAULT_SUBDIRS = {subdir: subdir
                       for subdir in ['inbox', 'queued', 'executed']}
    DEFAULT_LOCK_FILE_NAME = 'JOBMAN__LOCK'

    def __init__(self, *args, root_path=None, subdir_paths=None,
                 ensure_subdirs=True, job_spec_defaults=None,
                 lock_file_name=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.root_path = Path(root_path)
        self.subdir_paths = self._setup_subdir_paths(subdir_paths=subdir_paths)
        if ensure_subdirs:
            self._ensure_subdirs()
        self.job_spec_defaults = job_spec_defaults or {}
        self.lock_file_name = lock_file_name or self.DEFAULT_LOCK_FILE_NAME

    def _setup_subdir_paths(self, subdir_paths=None):
        return {
            subdir_key: Path(self.root_path, subdir_path)
            for subdir_key, subdir_path in ({
                **self.DEFAULT_SUBDIRS, **(subdir_paths or {})
            }).items()
        }

    def _ensure_subdirs(self):
        for subdir_path in self.subdir_paths:
            subdir_path.mkdir(parents=True, exist_ok=True)

    def tick(self):
        self._move_finished()
        self._ingest()

    def _move_finished(self):
        self._move_completed()
        self._move_failed()

    def _move_completed(self):
        pass

    def _move_failed(self):
        pass

    def _ingest(self, limit=None):
        count = 0
        for item_path in self.subdir_paths['inbox'].glob('*'):
            count += 1
            self._ingest_inbox_item(item_path=item_path)
            if limit and count > limit:
                break

    def _ingest_inbox_item(self, item_path=None):
        lock_path = item_path / self.lock_file_name
        if lock_path.exists():
            return
        lock_path.touch()
        dest_path_in_queued = self.subdir_paths['queued'] / item_path.name
        shutil.move(str(item_path), str(dest_path_in_queued))
        (dest_path_in_queued / self.lock_file_name).unlink()
        self.jobman.submit_job_dir(
            job_dir=str(dest_path_in_queued),
            source_key=self.key,
            job_spec_defaults=self.job_spec_defaults
        )
