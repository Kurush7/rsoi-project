import itertools
import sys
import os
from abc import abstractmethod

import pytest
from qr_server import IQRConfig, IQRManager, IQRLogger

cur = os.path.dirname(os.path.abspath(__file__))
sys.path.append(cur)
sys.path.append(cur + "/..")
sys.path.append(cur + "/../..")
sys.path.append(cur + "/../search")
sys.path.append(cur + "/../../common")

from SearchRepository import ISearchRepository
import unittest
from qr_server.Server import QRContext
from qr_server.Repository import IQRRepository
from search_service import *
from qr_server.TokenManager import MockTokenManager, ITokenManager
from test_tools import ContextCreator


class MockSearchRepository(ISearchRepository, IQRRepository):
    def connect_repository(self, config):
        pass

    def get_full_author(self, id):
        if id == 1: return {'type': 'author', 'id': id, 'description': 'description', 'name': 'name',
                            'photo': 'photo', 'birthdate': None, 'country': None}
        return None

    def get_full_series(self, id):
        if id == 1: return {'type': 'series', 'id': id, 'description': 'description', 'title': 'title',
                            'skin_image': 'skin_image', 'is_finished': False, 'books_count': 1, 'authors': []}
        return None

    def get_full_book(self, id):
        if id == 1: return {'type': 'book', 'id': id, 'book_order': None, 'description': 'description', 'title': 'title',
                            'skin_image': 'skin_image', 'genres': [], 'authors': [], 'series': None, 'publications': []}
        return None

    def get_filtered_books(self, filters: dict, offset=0, limit=100):
        return self._get_filtered(filters, filters['skip'], filters.get('book_id'), self._get_book_preview)

    def get_filtered_authors(self, filters: dict, offset=0, limit=100):
        return self._get_filtered(filters, filters['skip'], filters.get('author_id'), self._get_author_preview)

    def get_filtered_series(self, filters: dict, offset=0, limit=100):
        return self._get_filtered(filters, filters['skip'], filters.get('series_id'), self._get_series_preview)

    def _get_filtered(self, filters, skip, id, preview_getter):
        if filters.get('search'):
            if filters['search'] == 'abc':
                return [preview_getter(3)]
            return []
        if skip:
            return []
        if id:
            return [preview_getter(id)]
        return [preview_getter(id) for id in [1,2,3]]

    def _get_author_preview(self, id):
        return {"type": "author", "id": id, "name": "name", "photo": "photo"}

    def _get_series_preview(self, id):
        return {"type": "series", "id": id, "title": "title", "skin_image": "skin_image", "books_count": 0, "authors": []}

    def _get_book_preview(self, id):
        return {"type": "book", "id": id, "title": "title", "skin_image": "skin_image", "series": None, "authors": [], "book_order": 0}

    def set_role(self, role: str = 'guest'):
        pass

class MockKafkaProducer(IQRManager):
    def __init__(self):
        pass

    def connect(self, config):
        pass

    @staticmethod
    def get_name() -> str:
        return 'kafka_producer'

    def produce_dict(self, topic, value: dict, add_time=True, callback='default'):
        pass

    def produce(self, topic, value, callback='default'):
        pass

class MockTokenManager_v3(ITokenManager):
    def get_name(self):
        return 'jwt_token_validator'

    def load_config(self, config: IQRConfig):
        pass

    def make_token(self, user_id: int, payload: dict = None):
        return 'Bearer ' + str(user_id)

    @abstractmethod
    def validate_token(self, token, verify=True):
        if token is None or token == '':
            raise Exception('token expired')
        payload = {
            'sub': 'user',
            'email': 'a@a.ru',
            'nickname': 'a',
            'name': 'a',
            'family_name': 'a',
            'avatar': None,
        }
        return payload


def create_context(json_data=None, params=None, headers=None, form=None, files=None, auth_id=None):
    headers = headers if headers else dict()
    headers['Authorization'] = 'Bearer ' + str(auth_id if auth_id else 0)
    ctx = ContextCreator() \
        .context(json_data, params, headers, form, files) \
        .with_token_manager(MockTokenManager_v3(), auth_id) \
        .with_repository(MockSearchRepository()) \
        .with_manager(MockKafkaProducer()) \
        .build()
    return ctx


@pytest.mark.unit
class TestGetBook(unittest.TestCase):
    def test_success(self):
        res = book(create_context(), id=1)
        self.assertEqual(200, res.status_code)
        res.result.pop('skin_image')
        self.assertEqual({'type': 'book', 'id': 1, 'book_order': None, 'description': 'description', 'title': 'title',
                          'genres': [], 'authors': [], 'series': None, 'publications': []}, res.result)

    def test_not_found(self):
        res = book(create_context(), id=2)
        self.assertEqual(500, res.status_code)


@pytest.mark.unit
class TestGetSeries(unittest.TestCase):
    def test_success(self):
        res = series(create_context(), id=1)
        self.assertEqual(200, res.status_code)
        res.result.pop('skin_image')
        self.assertEqual({'type': 'series', 'id': 1, 'description': 'description', 'title': 'title',
                          'is_finished': False, 'books_count': 1, 'authors': []}, res.result)

    def test_not_found(self):
        res = series(create_context(), id=2)
        self.assertEqual(500, res.status_code)


@pytest.mark.unit
class TestGetAuthor(unittest.TestCase):
    def test_success(self):
        res = author(create_context(), id=1)
        self.assertEqual(200, res.status_code)
        res.result.pop('photo')
        self.assertEqual({'type': 'author', 'id': 1, 'description': 'description', 'name': 'name',
                          'birthdate': None, 'country': None}, res.result)

    def test_not_found(self):
        res = author(create_context(), id=2)
        self.assertEqual(500, res.status_code)


@pytest.mark.unit
class TestMain(unittest.TestCase):
    def _form_result(self, book_ids=None, author_ids=None, series_ids=None):
        res = []
        for id in book_ids if book_ids else []:
            res.append(MockSearchRepository()._get_book_preview(id))
        for id in series_ids if series_ids else []:
            res.append(MockSearchRepository()._get_series_preview(id))
        for id in author_ids if author_ids else []:
            res.append(MockSearchRepository()._get_author_preview(id))
        return res

    def _assert_main_results(self, res, origin):
        for x in itertools.chain(res, origin):
            for k in ['skin_image', 'photo']:
                if x.get(k) is not None:
                    x.pop(k)

        cmp = lambda x: str(x['id']) + x['type']
        res = sorted(res, key=cmp)
        origin = sorted(origin, key=cmp)
        self.assertEqual(res, origin)
        #self.assertEqual(set(map(frozenset, res)), set(map(frozenset, origin)))

    def test_all(self):
        res = main(create_context(params={'find_book': True, 'find_series': True, 'find_author': True}))
        self.assertEqual(200, res.status_code)
        self.assertEqual(9, len(res.result))
        self._assert_main_results(self._form_result([1,2,3], [1,2,3], [1,2,3]), res.result)

    def test_none(self):
        res = main(create_context(params={}))
        self.assertEqual(200, res.status_code)
        self.assertEqual(0, len(res.result))

    def test_books(self):
        res = main(create_context(params={'find_book': True}))
        self.assertEqual(200, res.status_code)
        self.assertEqual(3, len(res.result))
        self._assert_main_results(self._form_result([1,2,3], [], []), res.result)

    def test_authors(self):
        res = main(create_context(params={'find_author': True}))
        self.assertEqual(200, res.status_code)
        self.assertEqual(3, len(res.result))
        self._assert_main_results(self._form_result([], [1,2,3], []), res.result)

    def test_series(self):
        res = main(create_context(params={'find_series': True}))
        self.assertEqual(200, res.status_code)
        self.assertEqual(3, len(res.result))
        self._assert_main_results(self._form_result([], [], [1,2,3]), res.result)

    def test_book_by_id(self):
        res = main(create_context(params={'find_book': True, 'book_id': 1}))
        self.assertEqual(200, res.status_code)
        self.assertEqual(1, len(res.result))
        self._assert_main_results(self._form_result([1], [], []), res.result)

    def test_author_by_id(self):
        res = main(create_context(params={'find_author': True, 'author_id': 1}))
        self.assertEqual(200, res.status_code)
        self.assertEqual(1, len(res.result))
        self._assert_main_results(self._form_result([], [1], []), res.result)

    def test_series_by_id(self):
        res = main(create_context(params={'find_series': True, 'series_id': 1}))
        self.assertEqual(200, res.status_code)
        self.assertEqual(1, len(res.result))
        self._assert_main_results(self._form_result([], [], [1]), res.result)

    def test_search(self):
        res = main(create_context(params={'find_book': True, 'find_series': True, 'find_author': True, 'search': 'abc'}))
        self.assertEqual(200, res.status_code)
        self.assertEqual(3, len(res.result))
        self._assert_main_results(self._form_result([3], [3], [3]), res.result)