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
"""Utilities for working with Cassandra and the versions."""

import collections
import datetime
import logging
import grp
import os
import os.path
import pwd
import re
import socket

import dt_util
import file_util


class Components(object):
    """Constants for Cassandra SSTable components."""
    DATA = 'Data.db'


TEMPORARY_MARKERS = ["tmp", "tmplink"]
"""Marker used to identify temp sstables that are being created."""

TEMPORARY_SUFFIX = ".tmp"
"""Suffix of temporary files."""

FILE_VERSION_PATTERN = re.compile("[a-z]+")

log = logging.getLogger(__name__)

# ============================================================================
# Utility.

_SAFE_DT_FMT = "%Y_%m_%dT%H_%M_%S_%f"
"""strftime() format to safely use a datetime in file name."""


def _to_safe_datetime_fmt(dt):
    """Convert the datetime ``dt`` instance to a file system safe format.
    """
    return dt.strftime(_SAFE_DT_FMT)


def _from_safe_datetime_fmt(dt_str):
    """Convert the string ``dt_str`` from a file system safe format."""
    return datetime.datetime.strptime(dt_str, _SAFE_DT_FMT)


def is_backups_path(file_path):
    """Returns true if this path is a incremental backups path.

    It's a pretty simple test: does it have 'backups' in it.
    """
    return _contains_in_path(file_path, "backups")


def is_snapshot_path(file_path):
    """Returns true if this path is a snapshot path.

    It's a pretty simple test: does it have 'snapshots' in it.
    """
    return _contains_in_path(file_path, "snapshots")


def is_txn_log_path(file_path):
    """Returns true if this path is a txn log path.

    It's a pretty simple test: does it have 'snapshots' in it.
    """
    return file_path.endswith(".log") and "_txn_" in file_path


def _contains_in_path(file_path, component):
    head = os.path.dirname(file_path or "")
    if not head:
        raise ValueError("file_path %s does not include directory" %
                         (file_path, ))
    while head != "/":
        head, tail = os.path.split(head)
        if tail == component:
            return True
    return False


# ============================================================================
# On disk file stats.


class FileStat(object):
    """Basic file stats"""
    log = logging.getLogger("%s.%s" % (__name__, "FileStat"))

    def __init__(self,
                 file_path,
                 uid=None,
                 user=None,
                 gid=None,
                 group=None,
                 mode=None,
                 size=None):
        def meta():
            try:
                return meta.data
            except (AttributeError):
                meta.data = self._extract_meta(file_path)
            return meta.data

        self.file_path = file_path
        self.uid = meta()["uid"] if uid is None else uid
        self.gid = meta()["gid"] if gid is None else gid
        self.mode = meta()["mode"] if mode is None else mode
        self.size = meta()["size"] if size is None else size
        self.user = meta()["user"] if user is None else user
        self.group = meta()["group"] if group is None else group

    def __str__(self):
        return "FileStat for {file_path}: uid {uid}, user {user}, gid {gid},"\
            " group {group}, mode {mode}, size {size}".format(**vars(self))

    def serialise(self):
        """Serialise the state to a dict.

        Every value has to be a string or a dict.
        """
        return {
            "file_path": self.file_path,
            "uid": str(self.uid),
            "gid": str(self.gid),
            "mode": str(self.mode),
            "size": str(self.size),
            "user": str(self.user),
            "group": str(self.group),
        }

    @classmethod
    def deserialise(cls, data):
        """Create an instance use the ``data`` dict."""
        assert data

        def get_i(field):
            return int(data[field])

        return cls(
            data["file_path"],
            uid=get_i("uid"),
            user=data["user"],
            gid=get_i("gid"),
            group=data["group"],
            mode=get_i("mode"),
            size=get_i("size"))

    def _extract_meta(self, file_path):
        """Get a dict of the os file meta for the ``file_path``

        Allow OS errors to bubble out as files can be removed during
        processing.
        """

        stat = os.stat(file_path)
        file_meta = {
            "uid": stat.st_uid,
            "gid": stat.st_gid,
            "mode": stat.st_mode,
            "size": stat.st_size
        }

        try:
            file_meta['user'] = pwd.getpwuid(stat.st_uid).pw_name
        except (EnvironmentError, KeyError):
            log.debug("Ignoring error getting user name.", exc_info=True)
            file_meta['user'] = ""
        try:
            file_meta['group'] = grp.getgrgid(stat.st_gid).gr_name
        except (EnvironmentError, KeyError):
            log.debug("Ignoring error getting group name.", exc_info=True)
            file_meta['group'] = ""

        log.debug("For {file_path} got meta {file_meta} ".format(
            file_path=file_path, file_meta=file_meta))
        return file_meta


# ============================================================================
# Mock stats for a deleted file


class DeletedFileStat(FileStat):
    """File stats for a deleted file.

    Does not extract any data from disk.
    """
    log = logging.getLogger("%s.%s" % (__name__, "DeletedFileStat"))

    def __init__(self, file_path):
        super(DeletedFileStat, self).__init__(file_path)

    def _extract_meta(self, file_path):
        return {
            "uid": 0,
            "user": None,
            "gid": 0,
            "group": None,
            "mode": 0,
            "size": 0
        }


# ============================================================================
# A SSTable Component such as a -Data.db file.


class SSTableComponent(object):
    """Meta data about a component file for an SSTable.

    e.g. the -Data.db file.
    """
    log = logging.getLogger("%s.%s" % (__name__, "SSTableComponent"))

    def __init__(self,
                 file_path,
                 keyspace=None,
                 cf=None,
                 cf_id=None,
                 version=None,
                 index=None,
                 generation=None,
                 component=None,
                 temporary=None,
                 tableformat=None,
                 stat=None,
                 is_deleted=False):
        def props():
            try:
                return props.data
            except (AttributeError):
                props.data = self._component_properties(file_path)
            return props.data

        self.file_path = file_path
        self.keyspace = props()["keyspace"] if keyspace is None else keyspace
        self.cf = props()["cf"] if cf is None else cf
        self.cf_id = props()["cf_id"] if cf_id is None else cf_id
        self.version = props()["version"] if version is None else version
        self.index = props()["index"] if index is None else index
        self.generation = props()["generation"] if generation is None \
            else generation
        self.component = props()["component"] if component is None \
            else component
        self.temporary = props()["temporary"] if temporary is None \
            else temporary
        self.format = props()["format"] if tableformat is None \
            else tableformat

        self.is_deleted = is_deleted
        if stat is None:
            if self.is_deleted:
                self.stat = DeletedFileStat(file_path)
            else:
                self.stat = FileStat(file_path)
        else:
            self.stat = stat

    def __str__(self):
        return "SSTableComponent {file_path}".format(**vars(self))

    def serialise(self):
        """Serialise the state to a dict."""
        return {
            "keyspace": self.keyspace,
            "cf": self.cf,
            "cf_id": self.cf_id,
            "version": self.version,
            "index": self.index,
            "generation": str(self.generation),
            "component": self.component,
            "temporary": str(self.temporary),
            "format": self.format,
            "stat": self.stat.serialise(),
            "is_deleted": "true" if self.is_deleted else "false"
        }

    @classmethod
    def deserialise(cls, data):
        """Create an instance use the ``data`` dict."""
        assert data
        return cls(
            "",
            keyspace=data["keyspace"],
            cf=data["cf"],
            cf_id=data["cf_id"],
            version=data["version"],
            index=data["index"],
            generation=int(data["generation"]),
            component=data["component"],
            temporary=True if data["temporary"].lower() == "true" else False,
            tableformat=data["format"],
            stat=FileStat.deserialise(data["stat"]),
            is_deleted=True if data["is_deleted"] == "true" else False)

    def _component_properties(self, file_path):
        """Parses ``file_path`` to extact the component tokens.

        Raises :exc:`ValueError` if the ``file_path`` cannot be parsed.

        Reference: (for C* 3.11)
        https://github.com/apache/cassandra/blob/cassandra-3.11/src/java/org/apache/cassandra/io/sstable/Descriptor.java

        Returns a dict of the component properties.
        """
        self.log.debug("Parsing file path %s", file_path)

        file_dir, file_name = os.path.split(file_path)
        tokens = file_name.split("-")

        def pop():
            """Pop from the tokens.
            Expected a token to be there.
            """
            try:
                return tokens.pop()
            except (IndexError):
                raise ValueError("Not a valid SSTable file path %s" %
                                 (file_path, ))

        properties = {
            "component": pop(),
            "index": "",
            "temporary": file_name.endswith(TEMPORARY_SUFFIX),
        }

        format_or_gen = pop()
        if format_or_gen.isdigit():
            properties["generation"] = int(format_or_gen)
            properties["format"] = "legacy"
        else:
            properties["generation"] = int(pop())
            properties["format"] = format_or_gen

        properties['version'] = pop()

        # Older versions did not use two character file versions.
        if not FILE_VERSION_PATTERN.match(properties['version']):
            # If we cannot work out the version then we propably
            # decoded the file path wrong cause the cassandra version is wrong
            raise RuntimeError("Got invalid file version {version} for "
                               "file path {path}.".format(
                                version=properties['version'], path=file_path))

        if not properties['temporary'] and len(tokens) > 0:
            properties["temporary"] = pop() in TEMPORARY_MARKERS

        head, cf = os.path.split(file_dir)
        if cf.startswith('.'):
            properties['index'] = cf
            head, cf = os.path.split(head)

        properties['cf'], properties['cf_id'] = cf.split('-')
        _, properties['keyspace'] = os.path.split(head)

        self.log.debug("Got file properties %s from path %s", properties,
                       file_path)

        return properties

    @property
    def file_name(self):
        """Returns the file name for the componet formatted to the
        component version.
        """
        assert self.index == ""

        if self.format == "legacy":
            # Assume 2.1.x and beyond
            # file name adds the keyspace & cf
            fmt = "{keyspace}-{cf}-{version}-{generation}-{component}"
        else:
            # 3.x and beyond
            fmt = "{version}-{generation}-{format}-{component}"
        return fmt.format(**vars(self))

    @property
    def backup_file_name(self):
        """Returns the file name ot use when backing up this component.
        """
        # Assume 1.1 and beyond
        # file name adds the keyspace.
        return "{keyspace}-{cf}{index}-{version}-{generation}-{format}-{component}".format(
            **vars(self))

    def __hash__(self):
        return hash((self.keyspace, self.cf, self.cf_id, self.version, self.generation, self.component, self.temporary,
                     self.format, self.is_deleted, self.index))

    def same_sstable(self, other):
        """Returns ``True`` if the ``other`` :cls:`SSTableComponent`
        is from the same SSTable as this.
        """

        if other is None:
            return False

        return (other.keyspace == self.keyspace) and (
            other.cf == self.cf) and (other.version == self.version) and (
                other.generation == self.generation) and (
                    other.format == self.format) and (
                        other.index == self.index)


# ============================================================================
# A file that is going to be  or has been backed up.
# Includes the MD5 which is expensive to calculate.


class BackupFile(object):
    """A file that is going to be backed up
    """

    def __init__(self, file_path, host=None, md5=None, component=None):

        self.file_path = file_path
        self.component = SSTableComponent(file_path) if component is None \
            else component
        self.host = socket.getfqdn() if host is None else host
        self.md5 = file_util.file_md5(self.file_path) if md5 is None else md5

    def __str__(self):
        return "BackupFile {file_path}: host {host}, md5 {md5}, "\
            "{component}".format(**vars(self))

    def serialise(self):
        """Serialises the instance to a dict.

        All values must be string or dict.
        """

        return {
            "host": self.host,
            "md5": self.md5,
            "component": self.component.serialise()
        }

    @classmethod
    def deserialise(cls, data):
        """Deserialise the ``data`` dict to create a BackupFile."""

        assert data
        return cls(
            None,
            host=data["host"],
            md5=data["md5"],
            component=SSTableComponent.deserialise(data["component"]))

    @classmethod
    def backup_keyspace_dir(self, host, keyspace):
        """Gets the directory to that contains backups for the specified
        ``host`` and ``keyspace``.
        """

        return os.path.join(*("hosts", host, keyspace))

    @property
    def backup_path(self):
        """Gets the path to backup this file to.
        """

        return os.path.join(*(
            "hosts",
            self.host,
            self.component.keyspace,
            self.component.cf,
            self.component.backup_file_name,
        ))

    @property
    def restore_path(self):
        """Gets the path to restore this file.
        """

        # after 1.1  path was keyspace/cf/sstable
        return os.path.join(*(
            self.component.keyspace,
            self.component.cf,
            self.component.file_name,
        ))


# ============================================================================
# A file that was attempted to be restored.


class RestoredFile(object):
    """A file that was processed during a restore.

    It may or may not have been restored.
    """

    def __init__(self,
                 was_restored,
                 restore_path,
                 backup_file,
                 reason_skipped=None):
        self.was_restored = was_restored
        self.restore_path = restore_path
        self.backup_file = backup_file
        self.reason_skipped = reason_skipped

    def serialise(self):
        """Serialises the instance to a dict.

        All values must be string or dict.
        """

        return {
            "was_restored": "true" if self.was_restored else "false",
            "restore_path": self.restore_path,
            "backup_file": self.backup_file.serialise(),
            "reason_skipped": self.reason_skipped or ""
        }

    @classmethod
    def deserialise(cls, data):
        """Deserialise the ``data`` dict to create a :cls:`RestoredFile`."""

        assert data
        return cls(
            True if data["was_restored"] == "true" else False,
            data["restore_path"],
            BackupFile.deserialise(data["backup_file"]),
            reason_skipped=data["reason_skipped"])

    def restore_msg(self):
        """Small message describing where the file was restored from -> to."""

        if self.was_restored:
            return "{s.backup_file.backup_path} -> {s.restore_path}".format(
                s=self)
        return "{s.backup_file.backup_path} -> "\
            "Skipped: {s.reason_skipped}".format(s=self)


# ============================================================================
# A manifest of the files in a keyspace.


class KeyspaceBackup(object):
    """A backup set for a particular keyspace.
    """
    log = logging.getLogger("%s.%s" % (__name__, "KeyspaceBackup"))

    def __init__(self,
                 keyspace,
                 host=None,
                 timestamp=None,
                 backup_name=None,
                 components=None):
        # self.components.setdefault
        self.keyspace = keyspace
        self.host = host or socket.getfqdn()
        self.timestamp = timestamp or dt_util.now()
        self.backup_name = backup_name or "{ts}-{keyspace}-{host}".format(
            ts=_to_safe_datetime_fmt(self.timestamp),
            keyspace=keyspace,
            host=self.host)

        # Map of {cf_name : [component]}
        self.components = collections.defaultdict(set)
        if components:
            self.components.update(**components)

    def add_component(self, component):
        """Add the ``component`` to the list of components in this manifest.
        """
        assert component.keyspace == self.keyspace

        self.components[component.cf].add(component)
        return component

    def remove_sstable(self, component):
        """Remove all components for the SSTable ``component`` belongs to."""

        assert component.keyspace == component.keyspace
        for comp in self.components[component.cf].copy():
            if comp.same_sstable(component):
                self.components[component.cf].discard(comp)

    def snapshot(self):
        """Return a deep copy of this manifest that has the current
        timestamp."""

        return KeyspaceBackup(
            self.keyspace,
            host=self.host,
            components={cf: frozenset(components) for cf, components in self.components.iteritems()})

    def serialise(self):
        """Return manifest that desribes the backup set."""
        components = dict((key, [component.serialise() for component in value])
                          for key, value in self.components.iteritems())
        return {
            "host": self.host,
            "keyspace": self.keyspace,
            "timestamp": dt_util.to_iso(self.timestamp),
            "name": self.backup_name,
            "components": components
        }

    @classmethod
    def deserialise(cls, data):
        """Create an instance from the ``data`` dict. """

        assert data
        components = dict(
            (key, [SSTableComponent.deserialise(comp) for comp in value])
            for key, value in data["components"].iteritems())
        return cls(
            data["keyspace"],
            host=data["host"],
            timestamp=dt_util.from_iso(data["timestamp"]),
            backup_name=data["name"],
            components=components)

    @classmethod
    def from_backup_name(cls, backup_name):
        """Create a KeyspaceBackup from a backup name.

        The object does not contain a components list."""

        # format is timestamp-keyspace-host
        # host may have "-" parts so only split the first two tokens
        # from the name.
        tokens = backup_name.split("-", 2)
        assert len(tokens) == 3, "Invalid backup_name %s" % (backup_name, )
        safe_ts = tokens.pop(0)
        keyspace = tokens.pop(0)
        host = tokens.pop(0)
        assert not tokens

        # expecting 2012_10_22T14_26_57_871835 for the safe TS.
        timestamp = _from_safe_datetime_fmt(safe_ts)
        return cls(keyspace, host=host, timestamp=timestamp)

    @classmethod
    def from_backup_path(cls, backup_path):
        _, local = os.path.split(backup_path)
        backup_name, _ = os.path.splitext(backup_path)
        return cls.from_backup_name(backup_name)

    @classmethod
    def backup_keyspace_dir(cls, keyspace):
        """Returns the backup dir used for the ``keyspace``.

        Manifests are not stored in this path, they are in
        :attr:`backup_day_dir`
        """

        return os.path.join(*("cluster", keyspace))

    @classmethod
    def backup_day_dir(cls, keyspace, host, day):
        """Returns the backup dir used to store manifests for the
        ``keyspace`` and ``host`` on the datetime ``day``"""

        return os.path.join(*("cluster", keyspace, str(day.year),
                              str(day.month), str(day.day), host))

    @property
    def backup_path(self):
        """Gets the  path to backup the keyspace manifest to."""
        return os.path.join(
            self.backup_day_dir(self.keyspace, self.host, self.timestamp),
            "%s.mp.gz" % (self.backup_name, ))

    def iter_components(self):
        """Iterates through the SSTableComponents in this backup.

        Components ordered by column family. You will get all the components
        from "Aardvark" CF before "Beetroot"
        """

        cf_names = self.components.keys()
        cf_names.sort()
        for cf_name in cf_names:
            sorted_components = list(self.components[cf_name])
            sorted_components.sort(key=lambda x: x.file_name)
            for component in sorted_components:
                yield component
