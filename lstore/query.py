from lstore.table import Table, Record
from lstore.index import Index


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        record = self.find_base_record(primary_key)
        self.delete_loop(record.rid)

    def delete_loop(self, from_record):
        if from_record.indirection == -1:
            from_record.rid = -1
            return
        self.delete_loop(from_record.indirection)
        from_record.rid = -1

    def find_base_record(self, key):
        for i in range(self.table.farthest['pi'] + 1):
            page = self.table.page_directory[i]
            for j in range(self.table.farthest['slot_index'] + 1):
                if not self.check_record_validity(page.records[j], key) or not page.records[j].is_base:
                    continue
                return page.records[j]
        return None

    def find_first_free_slot(self):
        for pi in range(16):
            page = self.table.page_directory[pi]
            if not page.has_capacity():
                continue
            for i in range(len(page.data)):
                if i in page.records and page.records[i].rid != -1:
                    continue
                return {'pi': pi, 'slot_index': i}
        return None

    def check_is_farthest(self, slot):
        if (slot['pi'] > self.table.farthest['pi']) or (slot['pi'] == self.table.farthest['pi'] and slot['slot_index'] > self.table.farthest['slot_index']):
            self.table.farthest = slot

    def check_record_validity(self, record, key):
        if record.rid == -1 or record.get_key() != key:
            return False
        return True

    def get_record(self, rid):
        pi = 0
        slot_index = rid
        while slot_index > len(self.table.page_directory[0].data):
            slot_index -= len(self.table.page_directory[0].data)
            pi += 1
        return self.table.page_directory[pi].records[slot_index]

    def get_latest_version_rid(self, from_rid):
        indirection = self.get_record(from_rid).indirection
        if indirection == -1:
            return from_rid
        return self.get_latest_version_rid(indirection)

    def get_version_count(self, from_rid, count=1):
        indirection = self.get_record(from_rid).indirection
        if indirection == -1:
            return count
        return self.get_version_count(indirection, count + 1)

    def get_relative_version_rid(self, base_rid, relative_version):
        counter = self.get_version_count(base_rid) + relative_version
        return self.relative_version_rid_loop(base_rid, counter)

    def relative_version_rid_loop(self, from_rid, counter):
        indirection = self.get_record(from_rid).indirection
        if indirection == -1 or counter <= 1:
            return from_rid
        return self.get_relative_version_rid(indirection, counter - 1)

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *columns):
        slot = self.find_first_free_slot()
        self.check_is_farthest(slot)
        page = self.table.page_directory[slot['pi']]
        rid = slot['pi'] * len(page.data) + slot['slot_index']
        new_record = Record(rid, 0, columns)
        new_record.schema_encoding = '0' * self.table.num_columns
        page.write(new_record)
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
        selected = []
        for i in range(self.table.farthest['pi'] + 1):
            page = self.table.page_directory[i]
            for j in range(self.table.farthest['slot_index'] + 1):
                if not self.check_record_validity(page.records[j], search_key) or page.records[j].key != search_key_index:
                    continue
                selected.append(self.get_record(self.get_latest_version_rid(page.records[j].rid)))
                return selected
        return selected
    
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
        selected = []
        for i in range(self.table.farthest['pi'] + 1):
            page = self.table.page_directory[i]
            for j in range(self.table.farthest['slot_index'] + 1):
                if not self.check_record_validity(page.records[j], search_key) or page.records[j].key != search_key_index or not page.records[j].is_base:
                    continue
                selected.append(self.get_record(self.get_relative_version_rid(page.records[j].rid, relative_version)))
                return selected
        return selected

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        last_rid = -1
        for i in range(self.table.farthest['pi'] + 1):
            page = self.table.page_directory[i]
            for j in range(self.table.farthest['slot_index'] + 1):
                if not self.check_record_validity(page.records[j], primary_key) or not page.records[j].is_base:
                    continue
                last_rid = page.records[j].rid
                break
            if last_rid != -1:
                break
        if last_rid == -1:
            return False
        last_record = self.get_record(self.get_latest_version_rid(last_rid))
        slot = self.find_first_free_slot()
        self.check_is_farthest(slot)
        page = self.table.page_directory[slot['pi']]
        rid = slot['pi'] * len(page.data) + slot['slot_index']
        last_record.indirection = rid
        new_columns = []
        for i, v in enumerate(columns):
            if v is None:
                new_columns.append(last_record.columns[i])
            else:
                new_columns.append(v)
        new_record = Record(rid, 0, new_columns)
        new_record.is_base = False
        page.write(new_record)
        return True


    def check_range(self, start, end, v):
        if v < start or v > end:
            return False
        return True
    
    def get_values_from_range(self, start_range, end_range, key_index, search_index, relative_version=0):
        result = []
        for i in range(self.table.farthest['pi'] + 1):
            page = self.table.page_directory[i]
            for j in range(self.table.farthest['slot_index'] + 1):
                if page.records[j].rid == -1 or not page.records[j].is_base:
                    continue
                record = self.get_record(self.get_relative_version_rid(page.records[j].rid, relative_version))
                if not self.check_range(start_range, end_range, record.get_key()):
                    continue
                
                result.append(record.columns[search_index])
        return result

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        return (self.sum_version(start_range, end_range, aggregate_column_index, 0))
        pass

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
