from lstore.table import Table, Record
from lstore.page import Page
#import 

"""
Internal node object used for quickly traversing values stored in the index.
"""
class IndexNode:
    def __init__(self, in_value, previous_node, next_node, in_rid):
        self.value = in_value
        self.rid = in_rid
        self.prev = previous_node
        self.next = next_node

class IndexStore:
    def __init__(self):
        self.stored_records = [None]
        self.map_size = 10 # TODO: determine this value more intelligently?
        self.sorted_seeds = []
    
    # Finds the largest key that is smaller than the desired value.
    def find_largest_smaller_key(self, desired_value):
        for current_seed in self.sorted_seeds:
            if current_seed != None:
                continue
            if current_seed.next != None and current_seed.next.value >= desired_value:
                break
                
        desired_node = current_seed
        while desired_node != None and desired_node.next != None:
            if desired_node.next.main_key >= desired_value:
                break
            desired_node = desired_node.next
        
        return desired_node
    
    def insert_record(self, in_value, in_rid):
        bucket_placement = in_value % self.map_size
        self.stored_records[bucket_placement].append(in_rid)

        new_child_node = self.find_largest_smaller_key(in_value)
        IndexNode(in_value, new_child_node, new_child_node.next, in_rid)

        

"""
A data strucutre holding indices for various columns of a table. Key column should be indexed by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns

        for i in range(table.num_columns):
            self.create_index(i)

        pass


    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        
        # All data that maps to value should hash to indices[column]
        locations = self.indices[column].stored_records[value][:]

        # Remove unnecessary data points.
        for rec in locations:
            if rec.value != value:
                locations.remove(rec)

        return locations
        
    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        
        foundRIDs = []

        col_data = self.indices[column]
        desired_node = col_data.find_largest_smaller_key(begin)
        if not(desired_node is None):

          # the first node found is actually the one smaller than begin.
          desired_node = desired_node.next  
          while desired_node != None and desired_node.value <= end:
              foundRIDs.append(desired_node.rid)

        return foundRIDs

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        self.indices[column_number] = IndexStore()
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        # self.indices.pop(column_number)
        pass
