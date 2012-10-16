"""Sub Commands copy things locally
"""
import copy
import errno
import json
import logging
import os
import shutil
import socket

import file_util, subcommands

# ============================================================================
#

class LocalSnapSubCommand(subcommands.SnapSubCommand):
    """SubCommand to store SSTables locally
    """

    log = logging.getLogger("%s.%s" % (__name__, "LocalSnapSubCommand"))

    # command description used by the base 
    command_name = "snap-local"
    command_help = "Copy new SSTables to locally"
    command_description = "Copy new SSTables to locally"

    @classmethod
    def add_sub_parser(cls, sub_parsers):
        """Called to add a parser to ``sub_parsers`` for this command. 
        """
        
        parser = super(LocalSnapSubCommand, cls).add_sub_parser(sub_parsers)
        parser.add_argument('backup_base', default=None,
            help="Base destination path.")

        return parser

    def _create_worker_thread(self, i, file_queue, args):
        return LocalSnapWorkerThread(i, file_queue, args)

class LocalSnapWorkerThread(subcommands.SnapWorkerThread):
    log = logging.getLogger("%s.%s" % (__name__, "LocalSnapWorkerThread"))

    def _store(self, ks_manifest, cass_file):
        """Called up upload the ``cass_file``.
        """
        
        if self.is_file_stored(cass_file):
            # CRAP:
            self.log.warn("file "\
                "%(cass_file)s exists skipping" % vars())
            return

        # Store the keyspace manifest
        dest_manifest_path = os.path.join(self.args.backup_base, 
            ks_manifest.backup_path())

        if self.args.test_mode:
            self.log.info("TestMode -  store keyspace manifest to "\
                "%(dest_manifest_path)s" % vars())
        else:
            self._ensure_dir(dest_manifest_path)
            with open(dest_manifest_path, "w") as f:
                f.write(json.dumps(ks_manifest.manifest))
        
        # Store the cassandra file
        dest_file_path = os.path.join(self.args.backup_base, 
            cass_file.backup_path())

        if self.args.test_mode:
            self.log.info("TestMode - store file to %(dest_file_path)s"\
                 % vars())
        else:
            # Store the file
            dest_meta_path = "%(dest_file_path)s-meta.json" % vars()

            self._ensure_dir(dest_meta_path)
            with open(dest_meta_path, "w") as f:
                f.write(json.dumps(cass_file.file_meta))
            
            # copy the file
            shutil.copy(cass_file.file_path, dest_file_path)
        return


    def is_file_stored(self, cass_file):
        # HACK: 
        return False

    def _ensure_dir(self, path):
        if not os.path.isdir(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))
        return
    

# ============================================================================
#


class LocalListSubCommand(subcommands.ListSubCommand):
    """SubCommand to list backups
    """

    log = logging.getLogger("%s.%s" % (__name__, "LocalListSubCommand"))

    # command description used by the base 
    command_name = "list-local"
    command_help = "List local backup files"
    command_description = "List local backup files"

    @classmethod
    def add_sub_parser(cls, sub_parsers):
        """Called to add a parser to ``sub_parsers`` for this command. 
        """
        
        parser = super(LocalListSubCommand, cls).add_sub_parser(sub_parsers)
        parser.add_argument('backup_base', default=None,
            help="Base destination path.")

        return parser


    def _list_manifests(self):

        dest_manifest_path = os.path.join(self.args.backup_base, 
            file_util.KeyspaceManifest.keyspace_path(self.args.keyspace))

        _, _, all_files = os.walk(dest_manifest_path).next()
        host_files = [
            f
            for f in all_files
            if file_util.KeyspaceManifest.is_for_host(f, self.args.host)
        ]

        if self.args.list_all:
            return host_files

        return [max(host_files),]