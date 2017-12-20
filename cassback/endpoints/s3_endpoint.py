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

import argparse
import base64
import cStringIO
import errno
import json
import logging
import os.path
import threading

import botocore.config
import botocore.exceptions
import boto3.session
import boto3.s3.transfer

from cassback import cassandra, file_util
from cassback.endpoints import endpoints

# ============================================================================
# S3 endpoint


class S3Endpoint(endpoints.EndpointBase):

    log = logging.getLogger("%s.%s" % (__name__, "S3Endpoint"))
    name = "s3"
    bucket_lock = threading.Lock()

    def __init__(self, args):
        self.args = args

        self.log.info("Creating S3 connection.")
        self.session = boto3.session.Session(
            aws_access_key_id=args.aws_key,
            aws_secret_access_key=args.aws_secret,
            region_name=args.region,
            )
        self.core_config = botocore.config.Config(
            s3={'addressing_style': args.aws_addressing_style},
            retries={'max_attempts': args.retries},
        )
        self.resource = self.session.resource('s3', endpoint_url=args.s3_endpoint)
        self.client = self.resource.meta.client

        with S3Endpoint.bucket_lock:
            try:
                self.client.head_bucket(Bucket=args.bucket_name)
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] != "404":
                    raise

                self.log.debug("Creating S3 bucket %(bucket_name)s" % vars(self.args))
                options = {}
                if args.region is not None:
                    options['CreateBucketConfiguration'] = {}
                    options['CreateBucketConfiguration']['LocationConstraint'] = args.region
                self.resource.create_bucket(Bucket=args.bucket_name, **options)

        self.bucket = self.resource.Bucket(args.bucket_name)

        self.transfer_config = boto3.s3.transfer.TransferConfig(
            multipart_threshold=args.max_upload_size_mb * (1024**2),
            multipart_chunksize=args.multipart_chunk_size_mb * (1024**2),
            use_threads=False,
        )

        self.sse_options = {}
        if args.sse_c_key != "":
            self.sse_options = {
                'SSECustomerAlgorithm': 'AES256',
                'SSECustomerKey': base64.standard_b64decode(args.sse_c_key),
            }

    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # Endpoint Base Overrides

    @classmethod
    def add_arg_group(cls, main_parser):

        group = main_parser.add_argument_group(
            "S3 endpoint",
            description="Configuration for the AWS S3 endpoint.")

        group.add_argument(
            '--s3-endpoint',
            default=None,
            dest="s3_endpoint",
            help='S3 endpoint (overrides default).')
        group.add_argument(
            '--aws-key',
            dest='aws_key',
            default=None,
            help="AWS Access Key (picked up from the environment if not set)")
        group.add_argument(
            '--aws-secret',
            dest='aws_secret',
            default=None,
            help="AWS Access Secret Key (picked up from the environment if not set)")
        group.add_argument(
            '--aws-addressing-style',
            default='path',
            dest="aws_addressing_style",
            help='AWS addressing style (path, virtual or auto).')
        group.add_argument(
            '--region',
            default=None,
            dest="region",
            help='S3 region name for the bucket.')
        group.add_argument(
            '--bucket-name',
            default=None,
            dest="bucket_name",
            help='S3 bucket to upload to (created automatically if doesn\'t exist).')
        group.add_argument(
            '--key-prefix',
            default="",
            dest="key_prefix",
            help='S3 key prefix.')
        group.add_argument(
            '--sse-c-key',
            default="",
            dest="sse_c_key",
            help='S3 SSE-C encryption key, base64-encoded (enables server-side encryption).')

        group.add_argument(
            '--max-upload-size-mb',
            dest='max_upload_size_mb',
            type=int,
            default=5120,
            help='Max size for files to be uploaded before doing multipart ')
        group.add_argument(
            '--multipart-chunk-size-mb',
            dest='multipart_chunk_size_mb',
            default=256,
            type=int,
            help='Chunk size for multipart uploads (10%% of '
            'free memory if default is not available)')
        group.add_argument(
            '--retries',
            dest='retries',
            default=5,
            type=int,
            help='Number of times to retry s3 calls')

        return group

    @classmethod
    def validate_args(cls, args):

        if args.multipart_chunk_size_mb < 5:
            # S3 has a minimum.
            raise argparse.ArgumentTypeError("Minimum "
                                             "multipart_chunk_size_mb value is 5.")
        return

    def backup_file(self, backup_file):

        fqn = self._fqn(backup_file.backup_path)
        self.log.debug("Starting upload of %s to %s:%s",
                       backup_file, self.args.bucket_name, fqn)

        timing = endpoints.TransferTiming(self.log, fqn,
                                          backup_file.component.stat.size)

        self.bucket.upload_file(
            backup_file.file_path, fqn,
            ExtraArgs=dict(
                Metadata=self._dict_to_aws_meta(backup_file.serialise()),
                **self.sse_options
            ),
            Callback=timing.progress,
            Config=self.transfer_config)

        self.log.debug("Finished upload of %s to %s:%s",
                       backup_file, self.args.bucket_name, fqn)
        return fqn

    def read_backup_file(self, path):

        fqn = self._fqn(path)

        self.log.debug("Starting to read meta for key %s:%s ",
                       self.args.bucket_name, fqn)

        try:
            resp = self.client.head_object(Bucket=self.bucket.name, Key=fqn, **self.sse_options)

            return cassandra.BackupFile.deserialise(
                self._aws_meta_to_dict(resp['Metadata']))
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                raise EnvironmentError(errno.ENOENT, fqn)

            raise

    def backup_keyspace(self, ks_backup):
        fqn = self._fqn(ks_backup.backup_path)

        self.log.debug("Starting to store json to %s:%s",
                       self.args.bucket_name, fqn)

        json_str = json.dumps(ks_backup.serialise())

        with endpoints.TransferTiming(self.log, fqn, len(json_str)):
            self.bucket.put_object(
                Key=fqn,
                Body=json_str,
                ContentType='application/json',
                **self.sse_options
            )

        self.log.debug("Finished storing json to %s:%s", self.args.bucket_name,
                       fqn)
        return

    def read_keyspace(self, path):
        fqn = self._fqn(path)

        self.log.debug("Starting to read json from %s:%s",
                       self.args.bucket_name, fqn)

        with endpoints.TransferTiming(self.log, fqn, 0):
            try:
                body = self.bucket.Object(fqn).get(**self.sse_options)['Body']
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == "404":
                    raise EnvironmentError(errno.ENOENT, fqn)

                raise

        data = json.load(body)

        self.log.debug("Finished reading json from %s:%s",
                       self.args.bucket_name, fqn)

        return cassandra.KeyspaceBackup.deserialise(data)

    def restore_file(self, backup_file, dest_prefix):
        fqn = self._fqn(backup_file.backup_path)
        dest_path = os.path.join(dest_prefix, backup_file.restore_path)
        file_util.ensure_dir(os.path.dirname(dest_path))

        self.log.debug("Starting to restore from %s:%s to %s",
                       self.args.bucket_name, fqn, dest_path)

        timing = endpoints.TransferTiming(self.log, fqn,
                                          backup_file.component.stat.size)
        try:
            self.bucket.download_file(
                fqn, backup_file.file_path,
                ExtraArgs=self.sse_options,
                Callback=timing.progress,
                Config=self.transfer_config)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                raise EnvironmentError(errno.ENOENT, fqn)

            raise

        return dest_path

    def exists(self, relative_path):
        """Returns ``True`` if the file at ``relative_path`` exists. False
        otherwise.
        """

        fqn = self._fqn(relative_path)

        self.log.debug("Checking if key %s:%s exists", self.args.bucket_name,
                       fqn)

        try:
            self.client.head_object(Bucket=self.bucket.name, Key=fqn, **self.sse_options)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False

            raise

        return True

    def validate_checksum(self, relative_path, expected_hash):
        """Validates that the MD5 checksum of the file in the backup at
        ``relative_path`` matches ``expected_md5_hex``.
        """

        fqn = self._fqn(relative_path)

        self.log.debug("Starting to validate checkum for %s:%s",
                       self.args.bucket_name, fqn)

        try:
            response = self.client.head_object(Bucket=self.bucket.name, Key=fqn, **self.sse_options)
        except botocore.exceptions.ClientError:
            self.log.debug("Key %s does not exist, so checksum is invalid",
                           fqn)
            return False

        # original checked size, not any more.
        key_md5 = response['Metadata'].get('md5', '')
        if key_md5:
            hash_match = expected_hash == key_md5
        else:
            key_etag = response['ETag'].strip('"')
            self.log.info("Missing md5 meta data for %s using etag", fqn)
            hash_match = expected_hash == key_etag

        if hash_match:
            self.log.debug("Backup file %s matches expected md5 %s", fqn,
                           expected_hash)
            return True

        self.log.warn("Backup file %s does not match expected md5 "
                      "%s, got %s", fqn, expected_hash, key_md5 or key_etag)
        return False

    def iter_dir(self,
                 relative_path,
                 include_files=True,
                 include_dirs=False,
                 recursive=False):

        key_name = relative_path
        if not key_name.endswith("/"):
            key_name = key_name + "/"
        fqn = self._fqn(key_name)

        self.log.debug("Starting to iterate the dir for %s:%s",
                       self.bucket.name, fqn)

        if include_files and not include_dirs and not recursive:
            # easier, we just want to list the keys.
            key_names = [
                obj.key.replace(fqn, "")
                for obj in self.bucket.objects.filter(Prefix=fqn)
            ]
            if self.log.isEnabledFor(logging.DEBUG):
                self.log.debug("For dir %s:%s got files only %s",
                               self.bucket.name, fqn, key_names)
            return key_names

        items = []

        if not recursive:
            for page in self.client.get_paginator('list_objects_v2'). \
                    paginate(Bucket=self.bucket.name, Prefix=fqn, Delimiter="/"):
                # return files and/or directories in this path
                if include_files:
                    for key in page.get('Contents', []):
                        items.append(key['Key'].replace(fqn, ""))
                if include_dirs:
                    for directory in page.get('CommonPrefixes', []):
                        items.append(directory['Prefix'].replace(fqn, ""))

            if self.log.isEnabledFor(logging.DEBUG):
                self.log.debug("For dir %s:%s got non recursive dirs and/or "
                               "files %s", self.args.bucket_name, fqn, items)
            return items

        # recursive, we need to do a hierarchal list
        def _walk_keys(inner_key):
            for page in self.client.get_paginator('list_objects_v2'). \
                    paginate(Bucket=self.bucket.name, Prefix=inner_key, Delimiter="/"):

                for key in page.get('Contents', []):
                    yield key['Key']

                for directory in page.get('CommonPrefixes', []):
                    if include_dirs:
                        yield directory['Prefix']
                    for sub_entry in _walk_keys(directory['Prefix']):
                        yield sub_entry

        items = list(_walk_keys(fqn))
        if self.log.isEnabledFor(logging.DEBUG):
            self.log.debug("For dir %s:%s got recursive dirs and/or "
                           "files %s", self.bucket.name, fqn, items)
        return items

    def remove_file(self, relative_path, dry_run=False):
        """Removes the file at the ``relative_path``.

        Returns the full path to the file in the backup."""

        fqn = self._fqn(relative_path)

        if dry_run:
            return relative_path

        self.log.debug("Starting to delete key %s:%s" % (
            self.bucket.name,
            fqn,
        ))

        self.bucket.Object(fqn).delete()

        self.log.debug("Finished deleting from %s:%s", self.bucket.name,
                       fqn)
        return relative_path

    def remove_file_with_meta(self, relative_path, dry_run=False):
        """Removes the file at the ``relative_path`` that is expected to
        have meta data.

        Returns the fill path to the file in the backup."""

        # In S3 the meta is stored with the key.
        return self.remove_file(relative_path, dry_run=dry_run)

    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # Custom

    def _dict_to_aws_meta(self, data):
        """Turn a python dict into a dict suitable for use as S3 key meta.

        All values must be strings.
        Does not support multi levels, so create a single level pipe delim.
        """

        def add_meta(meta, key, value, context=None):
            if key:
                if key.find("|") > -1:
                    raise ValueError("Key cannot contain a '|' char, got "
                                     "%s" % (key,))
                fq_key = "{context}{sep}{key}".format(
                    context=context or "", sep="|" if context else "", key=key)
            else:
                fq_key = ""

            if isinstance(value, dict):
                for k, v in value.iteritems():
                    meta = add_meta(meta, k, v, context=fq_key)
                return meta

            elif isinstance(value, basestring):
                assert fq_key
                meta[fq_key] = value
                return meta
            else:
                raise ValueError("All values must be string or dict, got "
                                 "%s for %s" % (type(value), key))

        aws_meta = add_meta({}, None, data)
        self.log.debug("Converted data %s to aws_meta %s", data, aws_meta)
        return aws_meta

    def _aws_meta_to_dict(self, aws_meta):
        """Convert the aws meta to a multi level dict."""

        def set_value(data, key, value):
            head, _, tail = key.partition("|")
            if not tail:
                data[key] = value
                return
            # we have another level of dict
            data = data.setdefault(head, {})
            set_value(data, tail, value)
            return

        props = {}
        for k, v in aws_meta.iteritems():
            set_value(props, k.replace('-', '_'), v)
        return props

    def _fqn(self, key_name):
        """Returns fully qualified name for the bucket and key.

        Note the fully qualified name is not a url. It has the form
        <bucket_name>//key_path"""

        prefix = "%s/" % (self.args.key_prefix) if self.args.key_prefix\
            else ""
        return "%s%s" % (prefix, key_name)

    def _chunk_file(self, file_path):
        """Yield chunks from ``file_path``.
        """

        chunk_bytes = self.args.multipart_chunk_size_mb * (1024**2)

        self.log.debug("Splitting file %s into chunks of %s bytes", file_path,
                       chunk_bytes)

        with open(file_path, 'rb') as f:
            chunk = f.read(chunk_bytes)
            while chunk:
                yield cStringIO.StringIO(chunk)
                chunk = f.read(chunk_bytes)
        return
