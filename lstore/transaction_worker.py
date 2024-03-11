import threading

from lstore.table import Table, Record
from lstore.index import Index
from lstore import config

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = []):
        self.stats = []
        if len(transactions) == 0:
            self.transactions = []
        else:
            self.transactions = transactions
        self.result = 0
        self.t = None

    
    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)

        
    """
    Runs all transaction as a thread
    """
    def run(self):
        self.t = threading.Thread(target=lambda: self.__run())
        self.t.start()
        # here you need to create a thread and call __run


    """
    Waits for the worker to finish
    """
    def join(self):
        self.t.join()


    def __run(self):
        for i, transaction in enumerate(self.transactions):
            # each transaction returns True if committed or False if aborted
            self.stats.append(transaction.run())
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))