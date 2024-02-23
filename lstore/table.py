from lstore.index import Index
from lstore.config import *
from lstore.page import *
from lstore.bufferpool import BufferPool
from collections import defaultdict
from threading import Lock
import time
import os

# Record class
class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

# PageRange keeps track of base pages and tail pages
# Base/Tail pages are logical concepts
class PageRange:

    def __init__(self):
        self.base_page_index = 0 
        self.tail_page_index = 0 
        self.base_page = [None for _ in range(BASE_PAGE_MAX)]
        self.tail_page = [None] 
        
    def is_page_exist(self, index, type):
        if type == "base":
            return self.base_page[index] != None
        else:
            return self.tail_page[index] != None
        
    # content will be a page object(read from disk) if passed in    
    def create_base_page(self, index, content = None): 
        if content == None:
            self.base_page[index] = Page()
        else:
            self.base_page[index] = content

    def inc_base_page_index(self):
        self.base_page_index += 1

    def current_base_page(self):
        return self.base_page[self.base_page_index]

    def current_tail_page(self):
        return self.tail_page[self.tail_page_index]

    def add_tail_page(self):
        if self.tail_page[self.tail_page_index] == None:
            self.tail_page[self.tail_page_index] = Page()
        else:
            self.tail_page.append(Page())
            self.tail_page_index += 1
        

    def last_base_page(self):
        return self.base_page_index == BASE_PAGE_MAX - 1

class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def __init__(self, name, num_columns, key_column):
        self.table_path = ""
        self.name = name
        self.key_column = key_column
        self.num_columns = num_columns
        self.total_num_columns = self.num_columns+(META_COLUMN_COUNT + 1)
        self.page_directory = {}
        self.index = Index(self)
        self.num_records = 0
        self.num_updates = 0
        self.key_RID = {}
        self.farthest = {'pi': 0, 'slot_index': -1}

        # Create lock objects for multi-threading
        self.lock_manager = defaultdict()
        self.new_record = Lock()
        self.update_record = Lock()

    # Retrieve RID given a key
    def get_rid(self, key):
        return self.key_RID[key]
    
    # Set table path
    def set_path(self, path):
        self.table_path = path
        BufferPool.initial_path(self.table_path)

    # Use a number to find the location of the page 
    def determine_page_location(self, type):
        num_tail = self.num_updates
        num_base = self.num_records - num_tail
        page_range_index = num_base % RECORDS_PER_PAGE_RANGE
        if type == 'base':
            base_page_index = num_base % RECORDS_PER_PAGE
            return page_range_index, base_page_index
        else:
            tail_page_index = num_tail % RECORDS_PER_PAGE
            return page_range_index, tail_page_index

    # column is the insert data
    # Writing data to base page(done on original insertion of records)
    def base_write(self, columns):
        page_range_index, base_page_index = self.determine_page_location('base')
        for i, value in enumerate(columns):
            # Use buffer_id to find the page
            buffer_id = (self.name, "base", i, page_range_index, base_page_index)
            page = BufferPool.get_page(buffer_id)

            # If no room on page
            if not page.has_capacity():
                # If last base page, new page range
                if base_page_index == BASE_PAGE_MAX - 1:
                    page_range_index += 1
                    base_page_index = 0
                # Else, go to next base page
                else:
                    base_page_index += 1
                # Use buffer_id to find the page
                buffer_id = (self.name, "base", i, page_range_index, base_page_index)
            
            
            # Write in to page
            page.write(value)
            offset = page.num_records - 1
            BufferPool.add_pages(buffer_id, page)  
        
        # Write address into page directory
        rid = columns[RID_COLUMN]
        address = [self.name, "base", page_range_index, base_page_index, offset]
        self.page_directory[rid] = address
        self.key_RID[columns[self.key_column + (META_COLUMN_COUNT + 1)]] = rid
        self.num_records += 1
        self.index.push_index(columns[(META_COLUMN_COUNT + 1):len(columns) + 1], rid)
    
    # Writing data to tail pages
    def tail_write(self, columns):
        page_range_index, tail_page_index = self.determine_page_location('tail')
        for i, value in enumerate(columns):
            # Use buffer_id to find the page
            buffer_id = (self.name, "tail", i, page_range_index,tail_page_index)
            page = BufferPool.get_page(buffer_id)

            # If no room, go to next tail page
            if not page.has_capacity():
                tail_page_index += 1
                buffer_id = (self.name, "tail", i, page_range_index,tail_page_index)
                    
            page = BufferPool.get_page(buffer_id)
            # Write in to page
            page.write(value)
            offset = page.num_records - 1
            BufferPool.add_pages(buffer_id, page)
            
        # Write address into page directory
        rid = columns[RID_COLUMN]
        address = [self.name, "tail", page_range_index, tail_page_index, offset]
        self.page_directory[rid] = address
        self.key_RID[columns[self.key_column + (META_COLUMN_COUNT + 1)]] = rid
        self.num_records += 1
        self.num_updates += 1

    # Find RIDs given column index & target(key)
    def find_rids(self, column_index, target):
        rids = []
        # Retrieve records from page_directory
        for rid in self.page_directory:
            record = self.find_record(rid)
            # Check if matches target, add to rids
            if record[column_index + (META_COLUMN_COUNT + 1)] == target:
                rids.append(rid)
        return rids

    # Use Bufferpool to find value in a page & return
    def find_value(self, column_index, location):
        buffer_id = (location[0], location[1], column_index, location[2], location[3])
        page = BufferPool.get_page(buffer_id)
        value = page.get_value(location[4])
        return value
    
    # Find and update value in a page
    def update_value(self, column_index, location, value):
        buffer_id = (location[0], location[1], column_index, location[2], location[3])
        page = BufferPool.get_page(buffer_id)
        page.update(location[4], value)
        BufferPool.add_pages(buffer_id, page)

    # Find a record using the page directory & helper functions
    def find_record(self, rid):
        row = []
        location = self.page_directory[rid]
        # Iterate through non-metadata columns to grab record data
        for i in range((META_COLUMN_COUNT + 1) + self.num_columns):
            result = self.find_value(i, location)
            row.append(result)
        return row