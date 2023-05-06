import sys
import os

import pytest

cur = os.path.dirname(os.path.abspath(__file__))
sys.path.append(cur)
sys.path.append(cur + "/..")
sys.path.append(cur + "/../..")
sys.path.append(cur + "/../search")
sys.path.append(cur + "/../../common")

from SearchRepository import ISearchRepository
import unittest
from qr_server.Repository import IQRRepository
from search_service import *
from test_tools import DBTestManager, setUpDBTester


TEST_CONFIG_PATH = 'search_service/infrastructure/'
TEST_DATA_PATH = 'search_service/test/test_data/'


@pytest.mark.integration
class TestBooks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config = setUpDBTester(cls, TEST_CONFIG_PATH, with_proc=True)

        cls.repository = SearchRepository()
        cls.repository.connect_repository(config['database'])
        cls.dbtest_man.fill_data_from_dir(cls.repository.db, TEST_DATA_PATH)

    @classmethod
    def tearDownClass(cls):
        del cls.repository
        cls.dbtest_man.exit_test_db()

    def test_full_book_success(self):
        data = self.repository.get_full_book(1)
        self.assertEqual(data['id'], 1)
        self.assertEqual(len(data['authors']), 1)
        self.assertEqual(len(data['publications']), 1)
        self.assertIsNotNone(data['series'])

    def test_full_book_not_found(self):
        data = self.repository.get_full_book(10)
        self.assertIsNone(data)


@pytest.mark.integration
class TestAuthors(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config = setUpDBTester(cls, TEST_CONFIG_PATH, with_proc=True)

        cls.repository = SearchRepository()
        cls.repository.connect_repository(config['database'])
        cls.dbtest_man.fill_data_from_dir(cls.repository.db, TEST_DATA_PATH)

    @classmethod
    def tearDownClass(cls):
        del cls.repository
        cls.dbtest_man.exit_test_db()

    def test_full_author_success(self):
        data = self.repository.get_full_author(1)
        self.assertEqual(data['id'], 1)
        self.assertEqual(data['name'], 'author1')

    def test_full_author_not_found(self):
        data = self.repository.get_full_author(10)
        self.assertIsNone(data)


@pytest.mark.integration
class TestSeries(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config = setUpDBTester(cls, TEST_CONFIG_PATH, with_proc=True)

        cls.repository = SearchRepository()
        cls.repository.connect_repository(config['database'])
        cls.dbtest_man.fill_data_from_dir(cls.repository.db, TEST_DATA_PATH)

    @classmethod
    def tearDownClass(cls):
        del cls.repository
        cls.dbtest_man.exit_test_db()

    def test_full_series_success(self):
        data = self.repository.get_full_series(1)
        self.assertEqual(data['id'], 1)
        self.assertEqual(data['title'], 'series1')
        self.assertEqual(len(data['authors']), 1)
        self.assertEqual(data['authors'][0]['id'], 1)

    def test_full_series_not_found(self):
        data = self.repository.get_full_series(10)
        self.assertIsNone(data)


@pytest.mark.integration
class TestBookFilters(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config = setUpDBTester(cls, TEST_CONFIG_PATH, with_proc=True)

        cls.repository = SearchRepository()
        cls.repository.connect_repository(config['database'])
        cls.dbtest_man.fill_data_from_dir(cls.repository.db, TEST_DATA_PATH)

    @classmethod
    def tearDownClass(cls):
        del cls.repository
        cls.dbtest_man.exit_test_db()

    def test_skip(self):
        data = self.repository.get_filtered_books({'skip': True})
        self.assertEqual(len(data), 0)
        
    def test_limit(self):
        data = self.repository.get_filtered_books({}, limit=2)
        self.assertEqual(len(data), 2)
        
    def test_offset(self):
        data = self.repository.get_filtered_books({'sort': 'name_desc'}, offset=1)
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0]['title'], 'book2')
        self.assertEqual(data[1]['title'], 'book1')

    def test_by_book_id(self):
        data = self.repository.get_filtered_books({'book_id': 1})
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['id'], 1)

    def test_by_author_id(self):
        data = self.repository.get_filtered_books({'author_id': 2})
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['id'], 3)

    def test_by_series_id(self):
        data = self.repository.get_filtered_books({'series_id': 1})
        self.assertEqual(len(data), 2)
        self.assertNotIn('book3', [data[0]['title'], data[1]['title']])


@pytest.mark.integration
class TestBookFilters(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config = setUpDBTester(cls, TEST_CONFIG_PATH, with_proc=True)

        cls.repository = SearchRepository()
        cls.repository.connect_repository(config['database'])
        cls.dbtest_man.fill_data_from_dir(cls.repository.db, TEST_DATA_PATH)

    @classmethod
    def tearDownClass(cls):
        del cls.repository
        cls.dbtest_man.exit_test_db()

    def test_skip(self):
        data = self.repository.get_filtered_books({'skip': True})
        self.assertEqual(len(data), 0)

    def test_limit(self):
        data = self.repository.get_filtered_books({}, limit=2)
        self.assertEqual(len(data), 2)

    def test_offset(self):
        data = self.repository.get_filtered_books({'sort': 'name_desc'}, offset=1)
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0]['title'], 'book2')
        self.assertEqual(data[1]['title'], 'book1')

    def test_by_book_id(self):
        data = self.repository.get_filtered_books({'book_id': 1})
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['id'], 1)

    def test_by_author_id(self):
        data = self.repository.get_filtered_books({'author_id': 2})
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['id'], 3)

    def test_by_series_id(self):
        data = self.repository.get_filtered_books({'series_id': 1})
        self.assertEqual(len(data), 2)
        self.assertNotIn('book3', [data[0]['title'], data[1]['title']])

    def test_search(self):
        data = self.repository.get_filtered_books({'search': 'BoO'})    # part of 'book'
        self.assertEqual(len(data), 3)


@pytest.mark.integration
class TestAuthorFilters(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config = setUpDBTester(cls, TEST_CONFIG_PATH, with_proc=True)

        cls.repository = SearchRepository()
        cls.repository.connect_repository(config['database'])
        cls.dbtest_man.fill_data_from_dir(cls.repository.db, TEST_DATA_PATH)

    @classmethod
    def tearDownClass(cls):
        del cls.repository
        cls.dbtest_man.exit_test_db()

    def test_skip(self):
        data = self.repository.get_filtered_authors({'skip': True})
        self.assertEqual(len(data), 0)

    def test_limit(self):
        data = self.repository.get_filtered_authors({}, limit=1)
        self.assertEqual(len(data), 1)

    def test_offset(self):
        data = self.repository.get_filtered_authors({'sort': 'name_desc'}, offset=1)
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['name'], 'author1')

    def test_by_author_id(self):
        data = self.repository.get_filtered_authors({'author_id': 2})
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['id'], 2)

    def test_by_filtered_books(self):
        data = self.repository.get_filtered_authors({'filtered_books': [1, 3]})
        self.assertEqual(len(data), 2)

    def test_by_filtered_books2(self):
        data = self.repository.get_filtered_authors({'filtered_books': [1]})
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['id'], 1)


@pytest.mark.integration
class TestSeriesFilters(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config = setUpDBTester(cls, TEST_CONFIG_PATH, with_proc=True)

        cls.repository = SearchRepository()
        cls.repository.connect_repository(config['database'])
        cls.dbtest_man.fill_data_from_dir(cls.repository.db, TEST_DATA_PATH)

    @classmethod
    def tearDownClass(cls):
        del cls.repository
        cls.dbtest_man.exit_test_db()

    def test_skip(self):
        data = self.repository.get_filtered_series({'skip': True})
        self.assertEqual(len(data), 0)

    def test_limit(self):
        data = self.repository.get_filtered_series({}, limit=1)
        self.assertEqual(len(data), 1)

    def test_offset(self):
        data = self.repository.get_filtered_series({'sort': 'name_desc'}, offset=1)
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['title'], 'series1')

    def test_by_series_id(self):
        data = self.repository.get_filtered_series({'series_id': 2})
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['id'], 2)

    def test_by_author_id(self):
        data = self.repository.get_filtered_series({'author_id': 1})
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['id'], 1)

    def test_by_filtered_books(self):
        data = self.repository.get_filtered_series({'filtered_books': [1, 3]})
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['id'], 1)