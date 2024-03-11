import os

from lstore.index import Index
from lstore.page import Page, Record
#from time import time
from lstore import config
import math
import pickle
import threading


INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key, path = ''):
        self.path = path
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.insert_lock = threading.Lock()
        self.update_lock = threading.Lock()
        self.update_lineage_lock = threading.Lock()
        self.expand_base_lock = threading.Lock()
        self.index_lock = threading.Lock()
        self.page_directory = {}
        self.catalog = {'page_range': 0, 'stack_size': [], 'farthest': {'pi': 0, 'slot_index': -1}, 'num_columns': num_columns, 'key': key, 'update_count': []}
        self.index = Index(self)
        self.catalog['indices'] = self.index.indices

    #access the specified page. if in bufferpool, it's returned directly.
    #otherwise read from disk into bufferpool or make a new page if not exists.
    def get_page(self, page_id, page_index):
        key = f'{page_id}-{page_index}'
        if key in self.page_directory:
            return self.page_directory[key]
        if page_index == 0 and not os.path.exists(f'{self.path}/stack{page_id}'):
            self.expand_base(page_id)
            while not os.path.exists(f'{self.path}/stack{page_id}'):
                continue
            return self.page_directory[key]
        self.page_directory[key] = Page(self.path, page_id, page_index, self.num_columns, True)
        return self.page_directory[key]

    #create a new base page.
    def expand_base(self, pi=-1):
        page_id = self.catalog['page_range']
        if pi != -1:
            page_id = pi
        key = f'{page_id}-0'
        with self.expand_base_lock:
            if pi != -1 and pi < self.catalog['page_range']:
                return
            self.catalog['page_range'] += 1
            self.catalog['stack_size'].append(2)
            self.catalog['update_count'].append(0)
            tail_key = f'{page_id}-1'
            self.page_directory[tail_key] = Page(self.path, page_id, 1, self.num_columns)
            self.page_directory[key] = Page(self.path, page_id, 0, self.num_columns)
            os.mkdir(f'{self.path}/stack{page_id}')

    #create a new tail page.
    def add_tail(self, page_id):
        page_index = self.catalog['stack_size'][page_id]
        key = f'{page_id}-{page_index}'
        tail = Page(self.path, page_id, page_index, self.num_columns)
        self.page_directory[key] = tail
        self.catalog['stack_size'][page_id] += 1
        return tail

    #pin happens here on request for access
    def get_record(self, page_id, rid, projected_columns_index):
        slot_limit = math.floor(config.PAGE_SIZE / config.PAGE_SLOT_SIZE)
        page_index = math.floor(rid / slot_limit)
        slot_index = rid % slot_limit
        _page = self.get_page(page_id, page_index)
        _page.pin += 1
        record = _page.get_record(slot_index, projected_columns_index)
        _page.pin -= 1
        return record

    #merge a whole column into a base column.
    def merge_column(self, _page, _copy, page_id, column):
        projected_columns_index = [0] * (column + 1)
        projected_columns_index[column] = 1
        TPS = _page.TPS[column]
        for i in range(_page.num_records):
            record = _page.get_record(i, projected_columns_index)
            if record.indirection == -1:
                continue
            tail_record = self.get_record(page_id, record.indirection, projected_columns_index)
            if tail_record.rid > TPS:
                TPS = tail_record.rid
            _copy.records[i].columns[column] = tail_record.columns[column]
        _copy.TPS[column] = TPS

    #merge a whole page stack into a base page by merging the columns.
    def merge(self, page_id):
        #print("merge is happening")
        _page = self.get_page(page_id, 0)
        _copy = Page(_page.path, page_id, 0, _page.num_columns)
        _copy.num_records = _page.num_records
        for i in range(_page.num_records):
            _copy.records[i] = Record(i, self.key, [None] * _page.num_columns, _page.get_record(i, []).indirection)
        for column in range(_page.num_columns):
            self.merge_column(_page, _copy, page_id, column)
        _copy.dirty = True
        _copy.old = _page
        key = f'{page_id}-0'
        self.page_directory[key] = _copy

    #commit all the changes onto disk.
    def commit(self):
        with open(f'{self.path}/catalog.pickle', 'wb') as file:
            pickle.dump(self.catalog, file)
        for key, _page in self.page_directory.items():
            if _page.dirty and _page.pin == 0:
                _page.commit()
