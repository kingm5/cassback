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

import pkg_resources
import time

# ============================================================================
#


def create_from_args(args):

    endpoint_name = args.endpoint
    for entry_point in pkg_resources.iter_entry_points("cassback.endpoints"):

        endpoint_class = entry_point.load()
        if endpoint_class.name == endpoint_name:
            return endpoint_class(args)

    raise RuntimeError("Unknown endpoint name %s" % (endpoint_name, ))


def validate_args(args):

    endpoint_name = args.endpoint
    for entry_point in pkg_resources.iter_entry_points("cassback.endpoints"):

        endpoint_class = entry_point.load()
        if endpoint_class.name == endpoint_name:
            endpoint_class.validate_args(args)
            return

    raise RuntimeError("Unknown endpoint name %s" % (endpoint_name, ))


# ============================================================================
#


class EndpointBase(object):
    """Base for all endpoints."""

    name = None
    """Endpoint name, used in command line to identifity it.
    """

    @classmethod
    def add_arg_group(cls, main_parser):
        """
        """
        pass

    @classmethod
    def validate_args(cls, args):
        pass

    def backup_file(self, backup_file):
        """Store the SSTable ``backup_file``.

        Returns the fully qualified path to the file in the backup.
        """
        raise NotImplementedError()

    def read_backup_file(self, path):
        """Creates a :cls:`cassandra.BackupFile` from the meta for the file
        at ``path``
        """
        raise NotImplementedError()

    def backup_keyspace(self, ks_backup):
        raise NotImplementedError()

    def read_keyspace(self, path):
        raise NotImplementedError()

    def restore_file(self, backup_file, dest_prefix):
        """Restore the ``backup_file`` under the ``dest_prefix``.

        Returns the fully qualified backup path.
        """
        raise NotImplementedError()

    def exists(self, relative_path):
        """Returns ``True`` if the file at ``relative_path`` exists. False
        otherwise.
        """
        raise NotImplementedError()

    def validate_checksum(self, relative_path, expected_md5_hex):
        """Validates that the MD5 checksum of the file in the backup at
        ``relative_path`` matches ``expected_md5_hex``.
        """
        raise NotImplementedError()

    def iter_dir(self,
                 relative_path,
                 include_files=True,
                 include_dirs=False,
                 recursive=False):
        raise NotImplementedError()

    def remove_file(self, relative_path, dry_run=False):
        """Removes the file at the ``relative_path``.

        If ``dry_run`` the file is not deleted.

        Returns the fill path to the file in the backup."""
        raise NotImplementedError()

    def remove_file_with_meta(self, relative_path, dry_run):
        """Removes the file at the ``relative_path`` that is expected to
        have meta data.

        If ``dry_run`` the file is not deleted.

        Returns the fill path to the file in the backup."""
        raise NotImplementedError()


class TransferTiming(object):
    def __init__(self, logger, path, size):
        self.log = logger
        self.path = path
        self.start_ms = int(time.time() * 1000)
        self.size = size  # bytes

        # number of boto callbacks we should ask for.
        mb = 1024**2
        pattern = [
            (1 * mb, 0),  # 1MB, none
            (10 * mb, 1),  # 10MB, 1
            (100 * mb, 2),  # 100MB, 2
            (1024 * mb, 5),  # 1GB , 5
            (10 * 1024 * mb, 10),  # 10GB , 10
        ]

        self.num_callbacks = 20
        for i, j in pattern:
            if self.size < i:
                self.num_callbacks = j
                break

    def progress(self, progress):
        """Boto progress callback function.

        Logs the progress.
        """

        path = self.path
        elapsed_ms = int(time.time() * 1000) - self.start_ms
        throughput = ((progress * 1.0) /
                      (1024**2)) / ((elapsed_ms / 1000) or 1)

        if progress == self.size:
            pattern = "Transfered file {path} in {elapsed_ms:d} ms size "\
                      "{total} at {throughput:f} MB/sec"
        else:
            pattern = "Progress transfering file {path} elapsed "\
                      "{elapsed_ms:d} ms, transferred "\
                      "{progress} bytes at {throughput:f} MB/sec {total} "\
                      "total"

        self.log.info(pattern.format(path=path, elapsed_ms=elapsed_ms, throughput=throughput,
                                     progress=progress, total=self.size))
        return

    def __enter__(self):
        """Entry function when used as a context."""

        # Nothing to do.
        return self

    def __exit__(self, exc_type, exc_value, traceback):

        if exc_value is not None:
            # There was an error, let's just get out of here.
            return False

        # report 100% progress.
        self.progress(self.size)
        return False
