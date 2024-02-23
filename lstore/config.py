# Global Setting for the Database
# PageSize, StartRID, etc..

# Table values
PAGE_SIZE = 4096
PAGE_SLOT_SIZE = 8
BASE_PAGE_MAX = 16
META_COLUMN_COUNT = 4
SPECIAL_NULL_VALUE = pow(2, 64) - 1

# Column Indices
INDIRECTION_COLUMN = 0  # int
RID_COLUMN = 1  # int
TIMESTAMP_COLUMN = 2  # datetime
SCHEMA_ENCODING_COLUMN = 3  # string
KEY_COLUMN = META_COLUMN_COUNT