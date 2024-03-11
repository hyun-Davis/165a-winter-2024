import os.path
import shutil
import pickle

from lstore.table import Table

class Database():

    def __init__(self):
        self.path = './ECS165'
        self.tables = []

    # Not required for milestone1
    def open(self, path):
        self.path = path
        if not os.path.exists(self.path):
            os.mkdir(self.path)

    #commit all the changes on close.
    def close(self):
        for table in self.tables:
            table.commit()
        self.tables.clear()

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        path = f'{self.path}/{name}'
        if os.path.exists(path):
            self.drop_table(name)
        os.mkdir(path)
        table = Table(name, num_columns, key_index, path)
        self.tables.append(table)
        return table


    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        shutil.rmtree(f'{self.path}/{name}')

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        path = f'{self.path}/{name}'
        if not os.path.exists(path) or not os.path.exists(f'{path}/catalog.pickle'): return None
        with open(f'{path}/catalog.pickle', 'rb') as file:
            catalog = pickle.load(file)
        table = Table(name, catalog['num_columns'], catalog['key'], path)
        table.catalog = catalog
        table.index.indices = table.catalog['indices']
        self.tables.append(table)
        return table
