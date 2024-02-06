import lstore.table

"""
A data strucutre holding indices for various columns of a table. Key column should be indexed by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns

        # Functionality for storing the sorted seeds with which to quickly index
        self.sorted_seeds = []
        pass

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        
        locations = []

        column_data = self.indices[column]

        for indexedRecord in column_data:
            if indexedRecord.key == value:
                locations.append(indexedRecord.rid)
            

        '''target_key = self.sorted_seeds[0]
        for indexed_key in self.sorted_seeds:
            if indexed_key > value:
                break
            else:
                target_key = indexed_key'''    

        """
        while (next <= value && next exists):
          if target_key.value == value:
            
          else:
            taget_key = target_key.next
        """

        return locations
        
    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        
        foundRIDs = []

        for iRecord in self.indices[column].records:
            if iRecord.value <= begin and iRecord.value >= end:
                foundRIDs.append(iRecord.rid)
            
        return foundRIDs

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        # self.indices[column_number].append()
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        # self.indices.pop(column_number)
        pass
