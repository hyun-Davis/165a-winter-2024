import random
import math

"""
Internal node object used for quickly traversing values stored in the index.
"""
class IndexNode:
    def __init__(self, in_value, in_rid, page_id):
        self.value = in_value
        self.rid = [in_rid]
        self.page_id = [page_id]
        self.next = None

"""
Storage class for each column for indexing purposes.
"""
class IndexStore:

    def __init__(self):
        self.stored_records = {}
        self.first_node = None
        self.maximum_value = None
        self.sorted_seeds = []

        # to avoid conflict with tester's randomization seed
        self.RNG = random.Random()
    
    # Finds the largest key that is smaller than the desired value.
    def find_largest_smaller_key(self, desired_value):

        # To handle potential edge cases where the sorted seeds give nothing.
        desired_node = self.first_node

        # Parse through the sorted seeds list.
        for current_seed in self.sorted_seeds:
            if current_seed.value < desired_value:
                desired_node = current_seed


        # Parse for the node exactly before the closest value.
        while desired_node.next is not None:
            if desired_node.next >= desired_value:
                break
            desired_node = self.stored_records[desired_node.next]

        return desired_node

    def make_seeds(self):
        count = math.ceil(len(self.stored_records) / 100)
        seeds = self.RNG.sample(self.stored_records.items(), count)
        seeds_v = [s[0] for s in seeds]
        seeds_v.sort()
        self.sorted_seeds.clear()
        for v in seeds_v:
            self.sorted_seeds.append(self.stored_records[v])

    def insert_record(self, in_value, in_rid, page_id):

        if in_value in self.stored_records:
            self.stored_records[in_value].rid.append(in_rid)
            self.stored_records[in_value].page_id.append(page_id)
            return

        if (self.maximum_value is None) or (self.maximum_value < in_value):
            self.maximum_value = in_value

        new_node = IndexNode(in_value, in_rid, page_id)
        if self.first_node is None:
            # There is no first node, making this the first.
            self.first_node = new_node
        else:
            if self.first_node.value > in_value:
                # the new node is smaller than the current smallest.
                new_node.next = self.first_node.value
                self.first_node = new_node
            else:
                # insert the record's node into the list of nodes.
                new_child_node = self.find_largest_smaller_key(in_value)
                new_node.next = new_child_node.next
                new_child_node.next = new_node.value

        # Hash the rid into its storage location in the hash table
        self.stored_records[in_value] = new_node

        if len(self.stored_records) % 100 == 0:
            self.make_seeds()


"""
A data strucutre holding indices for various columns of a table. Key column should be indexed by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # store the table.
        self.table_ref = table

        # One index for each table. All our empty initially.
        self.indices = [None] * table.num_columns

        # initialize each column in the index storage.
        self.create_index(0)



    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        info = self.indices[column].stored_records[value]
        # All data that maps to value should hash to indices[column]
        return {'pi': info.page_id[0], 'rid': info.rid[0]}
        
    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        found = []

        col_data = self.indices[column]
        desired_node = col_data.find_largest_smaller_key(begin)
        if desired_node.value >= begin:
            for i in range(len(desired_node.rid)):
                found.append({'pi': desired_node.page_id[i], 'rid': desired_node.rid[i]})

        while desired_node.next is not None and desired_node.next <= end:
            # the first node found is actually the one smaller than begin.
            desired_node = col_data.stored_records[desired_node.next]
            for i in range(len(desired_node.rid)):
                found.append({'pi': desired_node.page_id[i], 'rid': desired_node.rid[i]})

        return found

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        created_store = IndexStore()
        self.indices[column_number] = created_store

        # catlogue the indices of each individual record.
        for i in range(self.table_ref.catalog['farthest']['pi'] + 1):
            page = self.table_ref.get_page(i, 0)
            for j in range(page.get_slot_limit()):
                if j not in page.records:
                    continue
                record = page.records[j]
                created_store.insert_record(record.columns[column_number], record.rid, i)

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number].clear()
