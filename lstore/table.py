import os

from lstore.index import Index
from lstore.page import Page, Record
from lstore.config import *
#from time import time
import pickle


class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key, path = ''):
        self.path = path
        self.page_partition = PAGE_SIZE
        self.slot_size = PAGE_SLOT_SIZE
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.catalog = {'page_range': 0, 'stack_size': [], 'farthest': {'pi': 0, 'slot_index': -1}, 'num_columns': num_columns, 'key': key}
        self.catalog['index'] = Index(self)

    #access the specified page. if in bufferpool, it's returned directly.
    #otherwise read from disk into bufferpool or make a new page if not exists.
    def get_page(self, page_id, page_index):
        key = f'{page_id}-{page_index}'
        if key in self.page_directory:
            return self.page_directory[key]
        if page_index == 0 and not os.path.exists(f'{self.path}/stack{page_id}'):
            self.expand_base()
            return self.page_directory[key]
        self.page_directory[key] = Page(self.path, page_id, page_index, self.num_columns, True)
        return self.page_directory[key]

    #create a new base page.
    def expand_base(self):
        page_id = self.catalog['page_range']
        key = f'{page_id}-0'
        self.page_directory[key] = Page(self.path, page_id, 0, self.num_columns)
        self.catalog['stack_size'].append(1)
        self.catalog['page_range'] += 1
        os.mkdir(f'{self.path}/stack{page_id}')

    #create a new tail page.
    def add_tail(self, page_id):
        page_index = self.catalog['stack_size'][page_id]
        key = f'{page_id}-{page_index}'
        tail = Page(self.path, page_id, page_index, self.num_columns)
        self.page_directory[key] = tail
        self.catalog['stack_size'][page_id] += 1
        return tail

    def __merge(self):
        print("merge is happening")
        pass

    #commit all the changes onto disk.
    def commit(self):
        with open(f'{self.path}/catalog.pickle', 'wb') as file:
            pickle.dump(self.catalog, file)
        for _page in self.page_directory.values():
            if _page.dirty:
                _page.commit()
