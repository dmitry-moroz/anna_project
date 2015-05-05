import time
from multiprocessing import Process, Manager
from config import cache_conf


class CacheMeta(type):

    cache_tout = cache_conf['cache_lifetime']
    func_names = ['get_data']
    cache = Manager().list()
    cache_cleaner = None

    def __new__(mcs, name, bases, dct):
        if CacheMeta.cache_cleaner is None:
            CacheMeta.cache_cleaner = Process(
                target=CacheMeta.cleaner,
                args=(CacheMeta.cache, CacheMeta.cache_tout)
            )
            CacheMeta.cache_cleaner.start()

        dct['cache'] = CacheMeta.cache

        for f_name in CacheMeta.func_names:
            if f_name in dct:
                dct[f_name] = CacheMeta.cache_func(dct[f_name])

        return super(CacheMeta, mcs).__new__(mcs, name, bases, dct)

    @staticmethod
    def cache_func(func):
        def cached(self, *args, **kwargs):
            if self.cache[:]:
                return self.cache[:]
            else:
                result = func(self, *args, **kwargs)
                self.cache.extend(result)
                return result
        return cached

    @staticmethod
    def cleaner(cache, t):
        while True:
            try:
                for i in xrange(t):
                    if not hasattr(cache, 'pop'):
                        raise AttributeError
                    time.sleep(1)
                print('[{0}] Clearing cache'.format(time.asctime()))
                while cache[:]:
                    cache.pop()
            except (AttributeError, IOError, KeyboardInterrupt):
                break
