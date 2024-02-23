from lstore.config import *
import math
import struct
import os

class Record:

    #key is the index of the primary key column
    def __init__(self, rid, key, columns, indirection=-1):
        self.rid = rid
        self.key = key
        self.columns = columns
        self.indirection = indirection
        self.schema_encoding = []

    #returns the actual key value
    def get_key(self):
        return self.columns[self.key]

class Page:

    def __init__(self, path, id, index, num_columns, read=False):
        self.slot_size = PAGE_SLOT_SIZE #number of bytes that can be stored in a slot
        self.num_columns = num_columns
        self.initialized = False
        self.dirty = False
        self.loaded = [0] * (self.num_columns + 1)
        if not read:
            self.loaded = [1] * (self.num_columns + 1)
        self.TPS = [-1] * (self.num_columns + 1)
        self.pin = 0
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)
        self.records = {}
        self.path = path
        self.id = id
        self.index = index
        if read:
            if not os.path.exists(self.get_file_path('info')):
                return
            with open(self.get_file_path('info'), "rb") as file:
                self.initialized = self.read_value_from_file(file, 0, 'bool')
                self.load_meta('rid')
                self.load_meta('key')
                self.load_meta('indirection')


    #maximum amount of slots for each physical page.
    def get_slot_limit(self):
        return math.floor(len(self.data) / self.slot_size)

    #whether or not the page has capacity to keep writing
    def has_capacity(self):
        return self.get_slot_limit() - self.num_records > 0

    #returns the location of the next available slot
    def find_first_free_slot(self, replace=False):
        if not replace: return self.num_records
        for i in range(self.get_slot_limit()):
            if i in self.records and self.records[i].rid != -1:
                continue
            return i
        return -1

    #check whether or not a data column has been loaded.
    def column_load_check(self, column):
        if self.loaded[column + 1] == 0:
            self.load_column(column)

    #check if a record object has been initialized.
    def record_load_check(self, position):
        if position not in self.records:
            self.records[position] = Record(-1, -1, [None] * self.num_columns)

    #load a metadata column.
    def load_meta(self, name):
        self.loaded[0] = 1
        with open(self.get_file_path(name), "rb") as file:
            byte_arr = self.read_column_bytes(file)
            for position in range(self.get_slot_limit()):
                byte_i = position * self.slot_size
                self.record_load_check(position)
                setattr(self.records[position], name, self.read_value(byte_arr[byte_i:byte_i+self.slot_size], 'int'))

    #load a data column
    def load_column(self, column):
        self.loaded[column + 1] = 1
        _type = self.get_column_type(column)
        with open(self.get_file_path(self.get_column_name(column)), "rb") as file:
            byte_arr = self.read_column_bytes(file)
            for position in range(self.get_slot_limit()):
                byte_i = position * self.slot_size
                self.record_load_check(position)
                self.records[position].columns[column] = self.read_value(byte_arr[byte_i:byte_i+self.slot_size], _type)

    #read the bytes of a physical page
    def read_column_bytes(self, file):
        file.seek(self.get_address(0))
        return file.read(len(self.data))

    #read the bytes in a slot at position.
    def read_bytes(self, file, position):
        file.seek(self.get_address(position))
        return file.read(self.slot_size)

    #read bytes into value.
    def read_value(self, _bytes, _type):
        if _type == 'int':
            return int.from_bytes(_bytes, 'big')
        elif _type == 'str':
            return _bytes.decode().strip('\0')
        elif _type == 'bool':
            return bool(struct.unpack('>Q', _bytes)[0])
        return None

    #write a value into bytes.
    def write_value(self, value):
        if isinstance(value, int):
            return value.to_bytes(8, 'big')
        elif isinstance(value, str):
            return value.encode().ljust(self.slot_size, b'\0')
        elif isinstance(value, bool):
            return struct.pack('>?', value)
        return None

    #read bytes from a specified position into value.
    def read_value_from_file(self, file, position, _type):
        return self.read_value(self.read_bytes(file, position), _type)

    #given a position, returns the physical address where the data is stored.
    def get_address(self, position):
        return (self.get_slot_limit() * self.index + position) * self.slot_size

    #returns the path of the requested binary file.
    def get_file_path(self, name):
        return f'{self.path}/stack{self.id}/{name}.bin'

    #given the index of a column, return its naming format on disk.
    def get_column_name(self, index):
        return f'column{index}'

    #returns the value type that the column corresponds to.
    def get_column_type(self, index):
        with open(self.get_file_path('info'), "rb") as file:
            return self.read_value_from_file(file, 1 + index, 'str')

    #retrieve a record containing values of specified columns at slot_index.
    def get_record(self, slot_index, projected_columns_index):
        for column, bit in enumerate(projected_columns_index):
            if bit == 1:
                self.column_load_check(column)
        return self.records[slot_index]

    #write a value to specified position.
    def write_to_file(self, name, value, position):
        address = self.get_address(position)
        path = self.get_file_path(name)
        if not os.path.exists(path):
            open(path, 'wb')
        with open(path, "r+b") as file:
            if value is None:
                return
            v = self.write_value(value)
            file.seek(address)
            file.write(v)

    #write bytes to specified position.
    def write_bytes_to_file(self, name, _bytes, position):
        address = self.get_address(position)
        path = self.get_file_path(name)
        if not os.path.exists(path):
            open(path, 'wb')
        with open(path, "r+b") as file:
            file.seek(address)
            file.write(_bytes)

    #write the specified metadata column onto disk.
    def write_meta_to_file(self, name):
        byte_arr = bytearray()
        for i in self.records:
            byte_arr.extend(self.write_value(getattr(self.records[i], name)))
        self.write_bytes_to_file(name, byte_arr, 0)

    #write the specified data column onto disk.
    def write_column_to_file(self, column):
        byte_arr = bytearray()
        for i in self.records:
            byte_arr.extend(self.write_value(self.records[i].columns[column]))
        self.write_bytes_to_file(self.get_column_name(column), byte_arr, 0)

    #write a record in memory (will only be commited after all transactions are finished)
    def write(self, value):
        if self.has_capacity():
            position = self.find_first_free_slot()
            self.records[position] = value
            self.num_records += 1
            if not self.initialized:
                self.initialized = True
                self.write_to_file('info', True, 0)
                for i, v in enumerate(value.columns):
                    self.write_to_file('info', str(type(v).__name__), 1 + i)
            if not self.dirty:
                self.dirty = True

    #commit all the changes onto disk.
    def commit(self):
        if not self.dirty:
            return
        self.write_meta_to_file('rid')
        self.write_meta_to_file('key')
        self.write_meta_to_file('indirection')
        for i in range(self.num_columns):
            self.write_column_to_file(i)

