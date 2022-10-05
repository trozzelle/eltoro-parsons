import os
import unittest
import requests_mock
from test.utils import assert_matching_tables
from test.test_eltoro.test_data import expected_campaigns, expected_orderlines, expected_orderline, expected_organizations, expected_creatives, expected_buckets, expected_stats
from parsons import Eltoro, Table

class TestEltoro(unittest.TestCase):

    def setUp(self):

        os.environ['ELTORO_API_USER'] = 'TESTUSER'
        os.environ['ELTORO_API_PASSWORD'] = 'TESTPASS'
        os.environ['ELTORO_API_TOKEN'] = 'TEST_AUTH_TOKEN'

        self.et = Eltoro()

    @requests_mock.Mocker()
    def test_get_campaigns(self, m):

        m.get(self.et.uri + 'campaigns', json=expected_campaigns)
        campaigns = self.et.get_campaigns()
        exp_tbl = Table(expected_campaigns['results'])
        assert_matching_tables(campaigns, exp_tbl)

    @requests_mock.Mocker()
    def test_get_orderlines(self, m):

        m.get(self.et.uri + 'orderlines', json=expected_orderlines)
        orderlines = self.et.get_orderlines()
        exp_tbl = Table(expected_orderlines['results'])
        assert_matching_tables(orderlines, exp_tbl)

    @requests_mock.Mocker()
    def test_get_orderline(self, m):

        test_id = 'TEST_ID'

        m.get(self.et.uri + 'orderlines/' + test_id, json=expected_orderline)
        orderline = self.et.get_orderline(test_id)
        exp_tbl = Table([expected_orderline])
        assert_matching_tables(orderline, exp_tbl)

    @requests_mock.Mocker()
    def test_get_organizations(self, m):

        m.get(self.et.uri + 'orgs', json=expected_organizations)
        organizations = self.et.get_organizations()
        exp_tbl = Table(expected_organizations['results'])
        assert_matching_tables(organizations, exp_tbl)

    @requests_mock.Mocker()
    def test_get_creatives(self, m):

        m.get(self.et.uri + 'creatives', json=expected_creatives)
        creatives = self.et.get_creatives()
        exp_tbl = Table(expected_creatives['results'])
        assert_matching_tables(creatives, exp_tbl)

    @requests_mock.Mocker()
    def test_get_buckets(self, m):

        m.get(self.et.uri + 'buckets', json=expected_buckets)
        buckets = self.et.get_buckets()
        exp_tbl = Table(expected_buckets['results'])
        assert_matching_tables(buckets, exp_tbl)
