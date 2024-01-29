
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)
        self.records = {}
        self.id = -1

    def has_capacity(self):
        return len(self.data) - self.num_records > 0

    def find_first_free_slot(self):
        for i in range(len(self.data)):
            if i in self.records and self.records[i].rid != -1:
                continue
            return i
        return -1

    def write(self, value):
        if self.has_capacity():
            index = self.find_first_free_slot()
            self.records[index] = value
            self.num_records += 1
