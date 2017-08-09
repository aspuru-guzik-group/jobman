from pathlib import Path
import shutil

from .base_job_source import BaseJobSource


class DirJobSource(BaseJobSource):
    DEFAULT_SUBDIRS = {
        subdir: subdir
        for subdir in ['inbox', 'queued', 'completed', 'failed']
    }
    DEFAULT_LOCK_FILE_NAME = 'JOBMAN__LOCK'
    FINISHED_TAG = 'FINISHED'

    def __init__(self, *args, root_path=None, subdir_paths=None,
                 ensure_subdirs=True, job_spec_defaults=None,
                 lock_file_name=None, transfer_fn=shutil.move, **kwargs):
        super().__init__(*args, **kwargs)
        self.root_path = Path(root_path)
        self.subdir_paths = self._setup_subdir_paths(subdir_paths=subdir_paths)
        if ensure_subdirs:
            self._ensure_subdirs()
        self.job_spec_defaults = job_spec_defaults or {}
        self.lock_file_name = lock_file_name or self.DEFAULT_LOCK_FILE_NAME
        self.transfer_fn = transfer_fn

    def _setup_subdir_paths(self, subdir_paths=None):
        return {
            subdir_key: Path(self.root_path, subdir_path)
            for subdir_key, subdir_path in ({
                **self.DEFAULT_SUBDIRS, **(subdir_paths or {})
            }).items()
        }

    def _ensure_subdirs(self):
        for subdir_path in self.subdir_paths.values():
            subdir_path.mkdir(parents=True, exist_ok=True)

    def tick(self):
        self._move_finished()
        self._ingest()

    def _move_finished(self):
        self._move_completed()
        self._move_failed()

    def _move_completed(self):
        self._move_jobs_to_subdir(
            jobs=self._get_unfinished_jobs_by_status(status='COMPLETED'),
            subdir_path=self.subdir_paths['completed']
        )

    def _move_jobs_to_subdir(self, jobs=None, subdir_path=None):
        for job in jobs:
            self._move_job_to_subdir(job=job, subdir_path=subdir_path)
        self.jobman.save_jobs(jobs=jobs)

    def _move_job_to_subdir(self, job=None, subdir_path=None):
        src = Path(job['job_spec']['dir'])
        dest = (subdir_path / src.name)
        try:
            self.transfer_fn(str(src), str(dest))
            job['job_spec']['dir'] = str(dest)
        except:
            self.jobman.logger.exception("error moving job")
        job['source_tag'] = self.FINISHED_TAG

    def _get_unfinished_jobs_by_status(self, status=None):
        return self.get_jobs(query={
            'filters': [
                self.jobman.generate_status_filter(status='COMPLETED'),
                {'field': 'source_tag', 'op': '!=', 'arg': self.FINISHED_TAG}
            ]
        })

    def _move_failed(self):
        self._move_jobs_to_subdir(
            jobs=self._get_unfinished_jobs_by_status(status='FAILED'),
            subdir_path=self.subdir_paths['failed']
        )

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
        self.transfer_fn(str(item_path), str(dest_path_in_queued))
        (dest_path_in_queued / self.lock_file_name).unlink()
        self.jobman.submit_job_dir(
            job_dir=str(dest_path_in_queued),
            source_key=self.key,
            job_spec_defaults=self.job_spec_defaults
        )
