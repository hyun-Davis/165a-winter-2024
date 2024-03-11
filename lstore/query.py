from lstore.table import Table, Record
from lstore.index import Index
import math
import time
import threading
from lstore import config
import os

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table


    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """

    def delete(self, primary_key):
        bases = self.find_base_record(primary_key, 0)
        for i in range(len(bases['pi'])):
            pi = bases['pi'][i]
            rid = bases['rid'][i]
            record = self.get_record(pi, rid, [])
            if record.indirection == -1:
                record.rid = -1
                continue
            self.delete_loop(pi, record)

    #set rids of all records for deletion following the lineage to -1
    def delete_loop(self, page_id, from_record):
        if from_record.indirection == -1:
            return
        from_record.rid = -1
        self.delete_loop(page_id, self.get_record(page_id, from_record.indirection, []))

    #given the key value and key column's index, return the corresponding base record's logical location.
    def find_base_record(self, key, key_index):
        projected_columns_index = [0] * (key_index + 1)
        projected_columns_index[key_index] = 0
        if self.table.index.indices[key_index] is not None:
            return self.table.index.locate(key_index, key)
        found = {'pi': [], 'rid': []}
        for i in range(self.table.catalog['farthest']['pi'] + 1):
            _page = self.table.get_page(i, 0)
            for j in range(_page.get_slot_limit()):
                if j not in _page.records or not self.check_record_validity(_page.get_record(j, projected_columns_index), key, key_index):
                    continue
                found['pi'].append(i)
                found['rid'].append(_page.records[j].rid)
        return found

    #returns the next available slot in base page.
    def find_first_free_slot(self, replace=False):
        pi = 0
        while True:
            page = self.table.get_page(pi, 0)
            if not page.has_capacity():
                pi += 1
                continue
            if not replace:
                return {'pi': pi, 'slot_index': page.num_records}
            for i in range(page.get_slot_limit()):
                if i in page.records and page.records[i].rid != -1:
                    continue
                return {'pi': pi, 'slot_index': i}
        return None

    #keep track of the farthest page stack and base slot.
    def check_is_farthest(self, slot):
        if (slot['pi'] > self.table.catalog['farthest']['pi']) or (slot['pi'] == self.table.catalog['farthest']['pi'] and slot['slot_index'] > self.table.catalog['farthest']['slot_index']):
            self.table.catalog['farthest'] = slot

    #given a record and key column's index, check if it matches the specified key value.
    def check_record_validity(self, record, key, key_index):
        if record.rid == -1 or record.columns[key_index] != key:
            return False
        return True

    #check if a value falls within the range from start to end
    def check_range(self, start, end, v):
        if v < start or v > end:
            return False
        return True

    #given page_id and rid, retrieve a record containing values of specified columns.
    def get_record(self, page_id, rid, projected_columns_index):
        return self.table.get_record(page_id, rid, projected_columns_index)

    #returns the latest version of a record by following indirection forward.
    def get_latest_version_rid(self, page_id, base_rid):
        return self.get_record(page_id, base_rid, []).indirection

    #returns the relative version of a record by following indirection backward.
    def get_relative_version_rid(self, page_id, base_rid, relative_version, projected_columns_index=[]):
        if relative_version == 0 and len(projected_columns_index) > 0:
            indirection = self.get_latest_version_rid(page_id, base_rid)
            _page = self.table.get_page(page_id, 0)
            in_base = True
            for column, bit in enumerate(projected_columns_index):
                if bit == 1 and indirection > _page.TPS[column]:
                    in_base = False
                    break
            if in_base:
                return base_rid
        return self.relative_version_rid_loop(page_id, self.get_latest_version_rid(page_id, base_rid), base_rid, relative_version)

    def relative_version_rid_loop(self, page_id, from_rid, base_rid, relative_version):
        indirection = self.get_record(page_id, from_rid, []).indirection
        if indirection == -1 or relative_version == 0:
            return from_rid
        return self.relative_version_rid_loop(page_id, indirection, base_rid, relative_version + 1)

    #get values from the requested column given the condition where its corresponding key column values are within a specified range.
    def get_values_from_range(self, start_range, end_range, key_index, search_index, relative_version=0):
        _range = max(key_index, search_index)
        projected_columns_index = [0] * (_range + 1)
        projected_columns_index[key_index] = 1
        projected_columns_index[search_index] = 1
        result = []
        if self.table.index.indices[key_index] is not None:
            info = self.table.index.locate_range(start_range, end_range, key_index)
            for base in info:
                result.append(self.get_record(base['pi'], self.get_relative_version_rid(base['pi'], base['rid'], relative_version, projected_columns_index), projected_columns_index).columns[search_index])
            return result
        for i in range(self.table.catalog['farthest']['pi'] + 1):
            page = self.table.get_page(i, 0)
            for j in range(page.get_slot_limit()):
                if j not in page.records or page.records[j].rid == -1:
                    continue
                record = self.get_record(i, self.get_relative_version_rid(i, page.records[j].rid, relative_version, projected_columns_index), projected_columns_index)
                if not self.check_range(start_range, end_range, record.columns[key_index]):
                    continue
                result.append(record.columns[search_index])
        return result


    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *columns):
        with self.table.insert_lock:
            self.table.catalog['farthest']['slot_index'] += 1
            slot_index = self.table.catalog['farthest']['slot_index']
            pi = self.table.catalog['farthest']['pi']
            if self.table.catalog['farthest']['slot_index'] >= config.SLOT_LIMIT - 1:
                self.table.catalog['farthest']['pi'] += 1
                self.table.catalog['farthest']['slot_index'] = -1
        page = self.table.get_page(pi, 0)
        rid = slot_index
        tail_rid = rid + config.SLOT_LIMIT
        new_record = Record(rid, 0, columns, tail_rid)
        new_record.schema_encoding = '0' * self.table.num_columns
        tail = self.table.get_page(pi, 1)
        page.num_records += 1
        page.write_at_position(new_record, slot_index)
        tail.num_records += 1
        tail.write_at_position(Record(tail_rid, 0, columns), slot_index)
        with self.table.index_lock:
            for i in range(len(columns)):
                if self.table.index.indices[i] is not None:
                    self.table.index.indices[i].insert_record(columns[i], rid, pi)
        return True

    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        return self.select_version(search_key, search_key_index, projected_columns_index, 0)

    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        bases = self.find_base_record(search_key, search_key_index)
        selected = []
        for i in range(len(bases['pi'])):
            pi = bases['pi'][i]
            rid = bases['rid'][i]
            record = self.get_record(pi, self.get_relative_version_rid(pi, rid, relative_version, projected_columns_index), projected_columns_index)
            if record.rid == -1:
                continue
            columns = []
            for column, bit in enumerate(projected_columns_index):
                if bit == 1:
                    columns.append(record.columns[column])
            selected.append(Record(rid, record.key, columns, record.indirection))
        return selected


    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        bases = self.find_base_record(primary_key, 0)
        if len(bases['pi']) == 0:
            return False
        projected_columns_index = [1] * len(columns)
        for i, v in enumerate(columns):
            if v is None:
                projected_columns_index[i] = 0
        pi = bases['pi'][0]
        base_rid = bases['rid'][0]
        base_record = self.get_record(pi, base_rid, [])
        with self.table.update_lock:
            index = self.table.catalog['stack_size'][pi] - 1
            if index <= 1:
                tail = self.table.add_tail(pi)
                index += 1
            else:
                tail = self.table.get_page(pi, index)
            if not tail.has_capacity():
                tail = self.table.add_tail(pi)
                index += 1
            slot_index = tail.num_records
            tail.num_records += 1
        last_rid = base_record.indirection
        last_record = self.get_record(pi, last_rid, [1] * self.table.num_columns)
        with self.table.update_lineage_lock:
            while last_record.update_in_progress:
                last_rid = self.get_latest_version_rid(pi, base_rid)
                last_record = self.get_record(pi, last_rid, projected_columns_index)
            last_record.update_in_progress = True
        rid = index * config.SLOT_LIMIT + slot_index
        base_record.indirection = rid
        new_columns = []
        for k, v in enumerate(columns):
            if v is None:
                new_columns.append(last_record.columns[k])
            else:
                new_columns.append(v)
        new_record = Record(rid, 0, new_columns, last_rid)
        if base_record.columns[0] is not None and new_columns[0] != base_record.columns[0]:
            v = new_columns[0]
            ind = self.table.index.indices[0]
            with self.table.index_lock:
                ind.remove(base_record.columns[0])
                base_record.columns[0] = v
                if v is not None:
                    ind.insert_record(v, base_rid, pi)
        tail.write_at_position(new_record, slot_index)
        last_record.update_in_progress = False
        self.table.catalog['update_count'][pi] += 1
        if self.table.catalog['update_count'][pi] >= 2048:
            self.table.catalog['update_count'][pi] = 0
            threading.Thread(target=lambda: self.table.merge(pi)).start()
        return True



    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        return self.sum_version(start_range, end_range, aggregate_column_index, 0)


    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        count = 0
        a = self.get_values_from_range(start_range, end_range, 0, aggregate_column_index, relative_version)
        for i in a:
            count += i
        return count

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
