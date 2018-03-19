"""Utilities for working with files."""

import errno
import glob
import logging
import os
import os.path
import shelve
import shutil
import stat as stat_fn
import tempfile
import threading
import time

from hashlib import md5

log = logging.getLogger(__name__)


def file_size(file_path):
    """Returns the byte size of a file at ``file_path``.
    """

    stat = os.stat(file_path)
    assert not stat_fn.S_ISDIR(stat.st_mode)
    return stat.st_size


def boto_compute_md5(fp, buf_size=8192, size=None):
    """
    Compute MD5 hash on passed file and return results in a tuple of values.
    :type fp: file
    :param fp: File pointer to the file to MD5 hash.  The file pointer
               will be reset to its current location before the
               method returns.
    :type buf_size: integer
    :param buf_size: Number of bytes per read request.
    :type size: int
    :param size: (optional) The Maximum number of bytes to read from
                 the file pointer (fp). This is useful when uploading
                 a file in multiple parts where the file is being
                 split inplace into different parts. Less bytes may
                 be available.
    :return: The hex digest version of the MD5 hash
    """
    return boto_compute_hash(fp, buf_size, size, hash_algorithm=md5)


def boto_compute_hash(fp, buf_size=8192, size=None, hash_algorithm=md5):
    hash_obj = hash_algorithm()
    spos = fp.tell()
    if size and size < buf_size:
        s = fp.read(size)
    else:
        s = fp.read(buf_size)
    while s:
        hash_obj.update(s)
        if size:
            size -= len(s)
            if size <= 0:
                break
        if size and size < buf_size:
            s = fp.read(size)
        else:
            s = fp.read(buf_size)
    hex_digest = hash_obj.hexdigest()
    fp.seek(spos)
    return hex_digest


def file_md5(file_path):
    """Returns a string Hex md5 digest for the file at ``file_path``."""
    log.debug("Calculating md5 for %s", file_path)
    start_ms = time.time() * 10**3
    fp = open(file_path, 'rb')
    try:
        # returns tuple (md5_hex, md5_base64, size)
        md5 = boto_compute_md5(fp)
    finally:
        fp.close()
    duration_ms = (time.time() * 10**3) - start_ms
    log.debug("Calculated hash %s for %s in %s ms", md5, file_path,
              duration_ms)
    return md5


def ensure_dir(path):
    """Ensure the directories for ``path`` exist.
    """

    try:
        os.makedirs(path)
    except (EnvironmentError) as e:
        if not (e.errno == errno.EEXIST and e.filename == path):
            raise
    return


def maybe_remove_dirs(path):
    """Like :func:`os.removedirs` but ignores the error if the directory
    is not empty.
    """

    try:
        os.removedirs(path)
    except (EnvironmentError) as e:
        if e.errno != errno.ENOTEMPTY:
            raise
    return


def human_disk_bytes(bytes):
    """Format the ``bytes`` as a human readable value.
    """
    patterns = [(1024.0**3, "G"), (1024.0**2, "M"), (1024.0, "K")]
    for scale, label in patterns:
        if bytes >= scale:
            return "{i:.1f}{label}".format(i=(bytes / scale), label=label)
    return "%sB" % (bytes, )


# ============================================================================
# Manages a stable hard link reference to a file


class FileReferenceContext(object):
    log = logging.getLogger("%s.%s" % (__name__, "FileReferenceContext"))

    def __init__(self, source_path, root_dir=None, temp_dir=None):
        """Creates a temporary hard link for the file at
        ``source_path``.

        The link is created when the context is entered and destroyed when
        it is left. You can also call the ``link`` and ``close`` functions
        to achieve the same result.

        The full (relative to ``root_dir``) path of the ``source_path``
        is re-created under a temporary directory so that we can parse
        the path name for information.
        """

        self._source_path = source_path
        self._root_dir = root_dir
        self._temp_dir = temp_dir
        self._stable_dir = None
        self.stable_path = None
        self.ignore_next_exit = False

    @classmethod
    def cleanup_temp_dir(cls, temp_dir):
        """Removes cassback temporary stuff in directory.
        """
        for directory in glob.glob(os.path.join(temp_dir, "cassback-*")):
            if os.path.isdir(directory):
                cls.log.warn("Cleaning up temporary directory %s", directory)
                shutil.rmtree(directory)

    def link(self):
        """Generates the stable link and returns it.

        If the link could not be generated because the source file was not
        found ``None`` is returned.

        Call ``close`` to delete the link."""

        self.__enter__()
        return self.stable_path

    def close(self):
        """Deletes the stable link."""

        self.__exit__(None, None, None)
        return

    def __enter__(self):
        """Enters the context and returns self if the link could be created.

        If the link could not be created because the source path did not
        exist ``None`` is returned.
        """
        if self.stable_path:
            return self.stable_path

        _, file_name = os.path.split(self._source_path)
        stable_dir = tempfile.mkdtemp(prefix="cassback-%s-" % file_name, dir=self._temp_dir)
        assert os.path.isabs(self._source_path)

        dest_path = os.path.relpath(self._source_path, self._root_dir if self._root_dir is not None else "/")
        stable_path = os.path.join(stable_dir, dest_path)

        self.log.debug("Linking %s to point to %s", stable_path,
                       self._source_path)
        ensure_dir(os.path.dirname(stable_path))
        try:
            os.link(self._source_path, stable_path)
        except (EnvironmentError) as e:
            if e.errno == errno.ENOENT:
                return None
            raise

        self._stable_dir = stable_dir
        self.stable_path = stable_path

        del self._temp_dir
        del self._root_dir
        del self._source_path

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):

        if self.ignore_next_exit:
            self.ignore_next_exit = False
            return False

        if self._stable_dir:
            self.log.debug("Deleting temp dir for link %s", self.stable_path)
            shutil.rmtree(self._stable_dir)
            self._stable_dir = None
            self.stable_path = None
        return False


class MD5Cache(object):
    """
    MD5Cache caches md5 for files by path/inode number and size.
    """
    log = logging.getLogger("%s.%s" % (__name__, "MD5Cache"))

    def __init__(self, db_directory):
        if db_directory is None:
            self.shelf = None
            return

        self.shelf = shelve.open(os.path.join(db_directory, "md5cache"))
        self.lock = threading.Lock()

    def compute_md5(self, file_path):
        if self.shelf is None:
            # passthrough
            return file_md5(file_path)

        stat = os.stat(file_path)
        key = os.path.basename(file_path) + "/" + str(stat.st_ino)

        with self.lock:
            ok = key in self.shelf

        if ok:
            with self.lock:
                size, md5 = self.shelf[key]

            if size == stat.st_size:
                self.log.debug("Using cached md5 for %s", file_path)
                return md5

            self.log.warn("Size mismatch for %s: %d != %d", file_path,
                          size, stat.st_size)

        md5 = file_md5(file_path)

        with self.lock:
            self.shelf[key] = (stat.st_size, md5)

        return md5
