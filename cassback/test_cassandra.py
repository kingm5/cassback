import unittest

import cassandra


class CassandraTestCase(unittest.TestCase):
    def test_is_snapshot_path(self):
        self.assertTrue(
            cassandra.is_snapshot_path(
                "/var/lib/cassandra/data/aosmd_us_east_2/object_cleanup_status-3fb68a8088e711e794a405e653bd2278/"
                "snapshots/1510168557464-object_cleanup_status/mc-5-big-CompressionInfo.db"
            ))

        self.assertFalse(
            cassandra.is_snapshot_path(
                "/var/lib/cassandra/data/aosmd_us_east_2/object_cleanup_status-3fb68a8088e711e794a405e653bd2278/"
                "mc-5-big-CompressionInfo.db"))

    def test_is_backups_path(self):
        self.assertTrue(
            cassandra.is_backups_path(
                "/var/lib/cassandra/data/aosmd_us_east_2/object_cleanup_status-3fb68a8088e711e794a405e653bd2278/"
                "backups/mc-5-big-CompressionInfo.db"))

        self.assertFalse(
            cassandra.is_backups_path(
                "/var/lib/cassandra/data/aosmd_us_east_2/object_cleanup_status-3fb68a8088e711e794a405e653bd2278/"
                "mc-5-big-CompressionInfo.db"))


class SSTableComponentTestCase(unittest.TestCase):
    def setUp(self):
        self.component = cassandra.SSTableComponent(
            file_path='xxx',
            keyspace='',
            cf='',
            version='',
            generation='',
            component='',
            temporary='',
            tableformat='',
            stat='',
            is_deleted='')

    def test_get_components_2x(self):
        self.assertEqual(
            self.component._component_properties(
                '/data/aosmd_us_east_1/objects-927c2cc0bb2e11e68e323918fdc525c0/'
                'aosmd_us_east_1-objects-ka-3-Data.db'), {
                    'cf': 'objects',
                    'component': 'Data.db',
                    'format': 'legacy',
                    'generation': 3,
                    'keyspace': 'aosmd_us_east_1',
                    'temporary': False,
                    'version': 'ka'
                })

    def test_get_components_3x(self):
        self.assertEqual(
            self.component._component_properties(
                '/data/aosmd_us_east_1/objects-27d7da9088e711e794a405e653bd2278/'
                'mc-33-big-Digest.crc32'), {
                    'cf': 'objects',
                    'component': 'Digest.crc32',
                    'format': 'big',
                    'generation': 33,
                    'keyspace': 'aosmd_us_east_1',
                    'temporary': False,
                    'version': 'mc'
                })
