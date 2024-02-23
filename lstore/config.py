# Global Setting for the Database
# PageSize, StartRID, etc..

# Table values
PAGE_SIZE = 4096
PAGE_RECORD_SIZE = 8
BASE_PAGE_MAX = 16
META_COLUMN_COUNT = 4
RECORDS_PER_PAGE = int(PAGE_SIZE / PAGE_RECORD_SIZE)        # How many records could fit on a page
RECORDS_PER_PAGE_RANGE = RECORDS_PER_PAGE * BASE_PAGE_MAX   # How many records could fit in a page range
SPECIAL_NULL_VALUE = pow(2, 64) - 1                         # Special Null value = Max integer

# Column Indices
INDIRECTION_COLUMN = 0          # int
RID_COLUMN = 1                  # int
TIMESTAMP_COLUMN = 2            # datetime
SCHEMA_ENCODING_COLUMN = 3      # string
KEY_COLUMN = META_COLUMN_COUNT

# Bufferpool Size
BUFFERPOOL_SIZE = 2000