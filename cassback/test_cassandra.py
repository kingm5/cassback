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

    def test_is_txn_log_path(self):
        self.assertTrue(
            cassandra.is_txn_log_path(
                "aosmd_us_geo_chcg01/object_versions-f13de5c0385511e6b0bd03fd330e462c/"
                "mc_txn_compaction_f3222060-e658-11e7-8131-7541dfba298f.log"))

        self.assertFalse(
            cassandra.is_txn_log_path(
                "/var/lib/cassandra/data/aosmd_us_east_2/object_cleanup_status-3fb68a8088e711e794a405e653bd2278/"
                "mc-5-big-CompressionInfo.db"))


class SSTableComponentTestCase(unittest.TestCase):
    def setUp(self):
        self.component = cassandra.SSTableComponent(
            file_path='xxx',
            keyspace='',
            cf='',
            cf_id='',
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
                    'cf_id': '927c2cc0bb2e11e68e323918fdc525c0',
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
                    'cf_id': '27d7da9088e711e794a405e653bd2278',
                    'component': 'Digest.crc32',
                    'format': 'big',
                    'generation': 33,
                    'keyspace': 'aosmd_us_east_1',
                    'temporary': False,
                    'version': 'mc'
                })
