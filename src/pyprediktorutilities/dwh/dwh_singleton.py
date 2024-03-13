from pyprediktorutilities import singleton
from pyprediktorutilities.dwh import dwh


class DwhSingleton(dwh.Dwh, metaclass=singleton.SingletonMeta):
    pass
