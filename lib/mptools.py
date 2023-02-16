##
##

import multiprocessing.queues


class MPValue(object):

    def __init__(self, i=0):
        self.count = multiprocessing.Value('i', i)

    def increment(self, i=1):
        with self.count.get_lock():
            self.count.value += i

    def decrement(self, i=1):
        with self.count.get_lock():
            self.count.value -= i

    def reset(self, i=0):
        with self.count.get_lock():
            self.count.value = i

    @property
    def next(self):
        with self.count.get_lock():
            self.count.value += 1
        return self.count.value

    @property
    def value(self):
        return self.count.value


class MPQueue(multiprocessing.queues.Queue):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._size = MPValue()

    def put(self, *args, **kwargs):
        self._size.increment()
        super().put(*args, **kwargs)

    def get(self, *args, **kwargs):
        self._size.decrement()
        return super().get(*args, **kwargs)

    def qsize(self):
        return self._size.value

    def empty(self):
        return True if self.qsize() == 0 else False

    def clear(self):
        while not self.empty():
            self.get()
