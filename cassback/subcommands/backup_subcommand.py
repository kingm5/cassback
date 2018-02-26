#!/usr/bin/env python
# encoding: utf-8

# Copyright 2012 Aaron Morton
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import copy
import logging
import os
import os.path
import Queue
import signal
import socket
import time

from watchdog import events, observers

from cassback import cassandra, file_util, healthcheck
from cassback.subcommands import subcommands

# ============================================================================
# Snap - used to backup files


class BackupSubCommand(subcommands.SubCommand):
    log = logging.getLogger("%s.%s" % (__name__, "BackupSubCommand"))

    command_name = "backup"
    command_help = "Backup SSTables"
    command_description = "backup SSTables"

    def __init__(self, args):
        self.args = args
        return

    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # Subcommand Overrides

    @classmethod
    def add_sub_parser(cls, sub_parsers):
        """
        """

        sub_parser = super(BackupSubCommand, cls).add_sub_parser(sub_parsers)

        sub_parser.add_argument(
            "--threads", type=int, default=4, help='Number of writer threads.')
        sub_parser.add_argument(
            "--report-interval-secs",
            type=int,
            default=5,
            dest="report_interval_secs",
            help='Interval to report on the size of the work queue.')

        sub_parser.add_argument(
            '--exclude-keyspace',
            dest='exclude_keyspaces',
            nargs="*",
            help="User keyspaces to exclude from backup.")
        sub_parser.add_argument(
            '--include-system-keyspace',
            default=False,
            dest='include_system_keyspace',
            action="store_true",
            help="Include the system keyspace.")

        sub_parser.add_argument(
            '--ignore-existing',
            default=False,
            dest='ignore_existing',
            action="store_true",
            help="Don't backup existing files.")
        sub_parser.add_argument(
            '--ignore-changes',
            default=False,
            dest='ignore_changes',
            action="store_true",
            help="Don't watch for file changes, exit immediately.")

        sub_parser.add_argument(
            "--cassandra-data-dir",
            default="/var/lib/cassandra/data",
            help="Top level Cassandra data directory.")
        sub_parser.add_argument(
            "--temp-dir",
            default=None,
            help="Temporary directory to link files to while uploading, it should reside on the same filesystem"
                 "as Cassandra data directory, if not set default temporary directory is used.")
        sub_parser.add_argument(
            "--cleanup-temp-dir",
            default=False,
            action="store_true",
            help="Clean up temporary directory on startup from leftover files.")

        sub_parser.add_argument(
            "--host",
            default=socket.getfqdn(),
            help="Host to backup this node as.")

        sub_parser.add_argument(
            "--scan-queue-limit",
            default=0,
            type=int,
            help="Size of scan queue limit, 0 means unbound.")

        sub_parser.add_argument(
            "--healthcheck-host",
            default='0.0.0.0',
            help="Host to bind for TCP healthcheck.")
        sub_parser.add_argument(
            "--healthcheck-port",
            default=0,
            type=int,
            help="Port to bind for TCP healthcheck, 0 disables healthcheck.")

        return sub_parser

    def __call__(self):
        self.log.info("Starting sub command %s", self.command_name)

        if self.args.healthcheck_port != 0:
            healthcheck.start(self.args.healthcheck_host, self.args.healthcheck_port)

        if self.args.temp_dir is not None:
            file_util.ensure_dir(self.args.temp_dir)

        if self.args.cleanup_temp_dir:
            file_util.FileReferenceContext.cleanup_temp_dir(self.args.temp_dir)

        # Make a queue, we put the files that need to be backed up here.
        file_q = Queue.Queue(self.args.scan_queue_limit)

        # Make a watcher
        watcher = WatchdogWatcher(
            self.args.cassandra_data_dir, file_q, self.args.ignore_existing,
            self.args.ignore_changes, self.args.exclude_keyspaces,
            self.args.include_system_keyspace,
            self.args.temp_dir)

        # Make worker threads
        self.workers = [
            self._create_worker_thread(i, file_q)
            for i in range(self.args.threads)
        ]
        for worker in self.workers:
            worker.start()

        if self.args.report_interval_secs > 0:
            reporter = SnapReporterThread(file_q,
                                          self.args.report_interval_secs)
            reporter.start()
        else:
            self.log.info("Progress reporting is disabled.")
        # Start the watcher
        watcher.start()

        self.log.info("Watcher finished, waiting for queue to be drained...")

        file_q.join()

        self.log.info("Finished sub command %s", self.command_name)

        # There is no message to call. Assume the process has been running
        # for a while.
        return (0, "")

    def _create_worker_thread(self, i, file_queue):
        """Creates a worker thread for the snap command
        """
        return SnapWorkerThread(i, file_queue, copy.copy(self.args))


# ============================================================================
# Worker thread for backing up files


class SnapWorkerThread(subcommands.SubCommandWorkerThread):
    log = logging.getLogger("%s.%s" % (__name__, "SnapWorkerThread"))

    def __init__(self, thread_id, file_q, args):
        super(SnapWorkerThread, self).__init__("SnapWorker-", thread_id)

        self.file_q = file_q
        self.args = args

    def _do_run(self):
        """Wait to get work from the :attr:`file_q`
        """

        endpoint = self._endpoint(self.args)
        while True:
            # blocking call
            backup_msg = self.file_q.get()
            if backup_msg.file_ref:
                # a file was created or renamed, we have a stable ref
                with backup_msg.file_ref:
                    self._run_internal(endpoint, backup_msg.ks_manifest,
                                       backup_msg.component)
            else:
                # A file was deleted, no stable ref.
                self._run_internal(endpoint, backup_msg.ks_manifest,
                                   backup_msg.component)
            self.file_q.task_done()
        return

    def _run_internal(self, endpoint, ks_manifest, component):
        """Backup the cassandra file and keyspace manifest.

        Let errors from here buble out.

        Returns `True` if the file was uploaded, `False` otherwise.
        """
        if component is None:
            # just backup the manifest
            endpoint.backup_keyspace(ks_manifest)
            return True

        self.log.info("Uploading file %s", component)

        if component.is_deleted:
            endpoint.backup_keyspace(ks_manifest)
            self.log.info("Uploaded backup set %s after deletion of %s",
                          ks_manifest.backup_path, component)
            return

        # Create a BackupFile, this will have checksums
        backup_file = cassandra.BackupFile(
            component.file_path, host=self.args.host, component=component)

        # Store the cassandra file
        if endpoint.exists(backup_file.backup_path):
            if endpoint.validate_checksum(backup_file.backup_path,
                                          backup_file.md5):

                self.log.info("Skipping file %s skipping as there is a "
                              "valid backup", backup_file)
            else:
                self.log.warn("Possibly corrupt file %s in the backup, "
                              "skipping.", backup_file)
            return False

        uploaded_path = endpoint.backup_file(backup_file)
        if ks_manifest is not None:
            endpoint.backup_keyspace(ks_manifest)

        self.log.info("Uploaded file %s to %s", backup_file.file_path,
                      uploaded_path)
        return True


# ============================================================================
# Reporter thread logs the number of pending commands.


class SnapReporterThread(subcommands.SubCommandWorkerThread):
    """Watches the work queue and reports on progress. """
    log = logging.getLogger("%s.%s" % (__name__, "SnapReporterThread"))

    def __init__(self, file_q, interval):
        super(SnapReporterThread, self).__init__("SnapReporter-", 0)
        self.interval = interval
        self.file_q = file_q

    def _do_run(self):

        last_size = 0
        while True:

            size = self.file_q.qsize()
            if size > 0 or (size != last_size):
                self.log.info("Backup worker queue contains %s items "
                              "(does not include tasks in progress)", size)
            last_size = size
            time.sleep(self.interval)
        return


# ============================================================================
# Watches the files system and queue's files for backup


class WatchdogWatcher(events.FileSystemEventHandler):
    """Watch the disk for new files."""
    log = logging.getLogger("%s.%s" % (__name__, "WatchdogWatcher"))

    def __init__(self, data_dir, file_queue, ignore_existing, ignore_changes,
                 exclude_keyspaces, include_system_keyspace, temp_dir):

        self.data_dir = data_dir
        self.file_queue = file_queue
        self.ignore_existing = ignore_existing
        self.ignore_changes = ignore_changes
        self.exclude_keyspaces = frozenset(exclude_keyspaces or [])
        self.include_system_keyspace = include_system_keyspace
        self.temp_dir = temp_dir

        self.keyspaces = {}

    def start(self):

        self.log.info("Refreshing existing files.")
        for root, dirs, files in os.walk(self.data_dir):
            for filename in files:
                self._maybe_queue_file(
                    os.path.join(root, filename),
                    enqueue=not (self.ignore_existing),
                    snapshot_ks=False)

        self.log.info("Uploading initial keyspace manifests.")

        for ks_manifest in self.keyspaces.values():
            self.file_queue.put(
                BackupMessage(None, ks_manifest.snapshot(), None))

        # watch if configured
        if self.ignore_changes:
            return

        observer = observers.Observer()
        observer.schedule(self, path=self.data_dir, recursive=True)
        self.log.info("Watching for new file under %s", self.data_dir)

        observer.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()
        observer.join(timeout=30)
        if observer.isAlive():
            self.log.error("Watchdog Observer failed to stop. Aborting.")
            os.kill(os.getpid(), signal.SIGKILL)
        return

    def _get_ks_manifest(self, keyspace):
        try:
            ks_manifest = self.keyspaces[keyspace]
        except (KeyError):
            ks_manifest = cassandra.KeyspaceBackup(keyspace)
            self.keyspaces[keyspace] = ks_manifest
        return ks_manifest

    def _maybe_queue_file(self, file_path, enqueue=True, snapshot_ks=True):
        if self.temp_dir is not None and os.path.commonprefix([file_path, self.temp_dir]) == self.temp_dir:
            # if temp_dir is under data_dir, skip temp files immediately
            return False

        with file_util.FileReferenceContext(file_path, root_dir=self.data_dir, temp_dir=self.temp_dir) as file_ref:
            if file_ref is None:
                # File was deleted before we could link it.
                self.log.info("Ignoring deleted path %s", file_path)
                return False

            if cassandra.is_snapshot_path(file_ref.stable_path):
                self.log.info("Ignoring snapshot path %s",
                              file_ref.stable_path)
                return False

            if cassandra.is_backups_path(file_ref.stable_path):
                self.log.info("Ignoring backups path %s",
                              file_ref.stable_path)
                return False

            if cassandra.is_txn_log_path(file_ref.stable_path):
                self.log.info("Ignoring txn log path %s",
                              file_ref.stable_path)
                return False

            try:
                component = cassandra.SSTableComponent(file_ref.stable_path)
            except (ValueError):
                self.log.info("Ignoring non Cassandra file %s",
                              file_ref.stable_path)
                return False

            if component.temporary:
                self.log.info("Ignoring temporary file %s",
                              file_ref.stable_path)
                return False

            if component.keyspace in self.exclude_keyspaces:
                self.log.info("Ignoring file %s from excluded "
                              "keyspace %s", file_ref.stable_path, component.keyspace)
                return False

            if (component.keyspace.lower().startswith("system")) and (
                    not self.include_system_keyspace):

                self.log.info("Ignoring system keyspace file %s",
                              file_ref.stable_path)
                return False

            # Update the manifest with this component
            ks_manifest = self._get_ks_manifest(component.keyspace)
            ks_manifest.add_component(component)
            if enqueue:
                self.log.info("Queueing file %s", file_ref.stable_path)
                self.file_queue.put(
                    BackupMessage(file_ref, ks_manifest.snapshot() if snapshot_ks else None, component))
                # Do not delete the file ref when we exit the context
                file_ref.ignore_next_exit = True

        return True

    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # Watchdog file events.

    def on_created(self, event):
        self._maybe_queue_file(event.src_path)
        return

    def on_moved(self, event):
        if self.temp_dir is not None and os.path.commonprefix([event.src_path, self.temp_dir]) == self.temp_dir:
            # if temp_dir is under data_dir, skip temp files immediately
            return

        self._maybe_queue_file(event.dest_path)
        return

    def on_deleted(self, event):
        # Deletes happen quickly when compaction completes.
        # Rather than do a backup for each file we only backup when
        # a -Data.db component is deleted.

        file_path = event.src_path

        if self.temp_dir is not None and os.path.commonprefix([file_path, self.temp_dir]) == self.temp_dir:
            # if temp_dir is under data_dir, skip temp files immediately
            return

        if cassandra.is_snapshot_path(file_path):
            self.log.info("Ignoring deleted snapshot path %s", file_path)
            return

        if cassandra.is_backups_path(file_path):
            self.log.info("Ignoring backups path %s",
                          file_path)
            return False

        if cassandra.is_txn_log_path(file_path):
            self.log.info("Ignoring txn log path %s",
                          file_path)
            return False

        try:
            component = cassandra.SSTableComponent(file_path, is_deleted=True)
        except (ValueError):
            self.log.info("Ignoring deleted non Cassandra file %s", file_path)
            return

        if component.component != cassandra.Components.DATA:
            self.log.info("Ignoring deleted non %s component %s",
                          cassandra.Components.DATA, file_path)
            return

        # We want to do a backup so we know this sstable was removed.
        ks_manifest = self._get_ks_manifest(component.keyspace)
        ks_manifest.remove_sstable(component)
        self.log.info("Queuing backup after deletion of %s", file_path)
        self.file_queue.put(
            BackupMessage(None, ks_manifest.snapshot(), component))
        return


# ============================================================================
# Message passed to the backup threads


class BackupMessage(object):
    def __init__(self, file_ref, ks_manifest, component):
        self.file_ref = file_ref
        self.ks_manifest = ks_manifest
        self.component = component
