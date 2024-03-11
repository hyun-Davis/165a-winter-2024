# Global Setting for the Database
# PageSize, StartRID, etc..

# Table values
PAGE_SIZE = 4096
PAGE_SLOT_SIZE = 8
SLOT_LIMIT = int(PAGE_SIZE / PAGE_SLOT_SIZE)

# Column Indices
INDIRECTION_COLUMN = 0  # int
RID_COLUMN = 1  # int
TIMESTAMP_COLUMN = 2  # datetime
SCHEMA_ENCODING_COLUMN = 3  # string

# Lock
SHARED_LOCK = 1
EXCLUSIVE_LOCK = 2