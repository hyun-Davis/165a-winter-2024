from lstore.table import Table, Record
from lstore.page import Page
import random
#import 

"""
Internal node object used for quickly traversing values stored in the index.
"""
class IndexNode:
    def __init__(self, in_value, in_rid):
        self.value = in_value
        self.rid = in_rid
        self.next = None

"""
Storage class for each column for indexing purposes.
"""
class IndexStore:

    def __init__(self):
        self.stored_records = {}
        self.first_node = None
        self.maximum_value = None
        self.sorted_seeds = {}
    
    # Finds the largest key that is smaller than the desired value.
    def find_largest_smaller_key(self, desired_value):

        # To handle potential edge cases where the sorted seeds give nothing.
        desired_node = self.first_node

        # Parse through the sorted seeds list.
        for current_seed in self.sorted_seeds:
            if current_seed != None:
                continue
            if current_seed.next != None and current_seed.next.value >= desired_value:
                break
        
        if not (current_seed is None):
            # Find the exact value that is just before where this node should be.
            desired_node = current_seed

        # Parse for the node exactly before the closest value.
        while desired_node != None and desired_node.next != None:
            if desired_node.next.main_key >= desired_value:
                break
            desired_node = desired_node.next
        
        return desired_node
    
    def insert_record(self, in_value, in_rid):

        if (self.maximum_value is None) or (self.maximum_value < in_value):
            self.maximum_value = in_value

        new_node = IndexNode(in_value, in_rid)
        if self.first_node is None:
            # There is no first node, making this the first.
            self.first_node = new_node
        else:
            if self.first_node.value > in_value:
                # the new node is smaller than the current smallest.
                new_node.next = self.first_node
                self.first_node = new_node
            else:
                # insert the record's node into the list of nodes.
                new_child_node = self.find_largest_smaller_key(in_value)
                new_node.next = new_child_node.next
                new_child_node.next = new_node

        # Hash the rid into its storage location in the hash table
        self.stored_records[in_value].append(new_node)

        # Update the sorted seeds.
        """
        Randomly step through values up to max_node divided by some value.
        """
        max_node = self.find_largest_smaller_key(self.maximum_value + 1)
        # TODO: more intelligent addition of values.
        i = self.first.node.value + random.randint(1, max_node.value / 5)

        self.sorted_seeds.clear()

        # Sequentially add to sorted_seeds until the maximum node value.
        while i < max_node.value:
            
            # As a set, sorted_seeds should reject duplicates.
            self.sorted_seeds.add(self.find_largest_smaller_key(i))
        
        self.sorted_seeds.sort()



        

"""
A data strucutre holding indices for various columns of a table. Key column should be indexed by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # store the table.
        self.table_ref = table

        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns

        # initialize each column in the index storage.
        for i in range(table.num_columns):
            self.create_index(i)
                
        pass


    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        
        # All data that maps to value should hash to indices[column]
        return self.indices[column].stored_records[value][:]
        
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
        created_store = IndexStore()
        self.indices[column_number] = created_store

        # catlogue the indices of each individual record.
        for i in range(self.table.farthest['pi'] + 1):
            page = self.table.page_directory[i]
            for j in range(self.table.farthest['slot_index'] + 1):
                record = page.records[j]

                created_store.insert_record(record.columns[column_number], record.rid)
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        # self.indices.pop(column_number)
        pass
