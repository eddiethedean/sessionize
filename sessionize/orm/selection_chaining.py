def selection_chaining(cls):
    class InnerCls(cls):
        def __getitem__(self, key):
            if isinstance(key, tuple):
                result = self
                for item in key:
                    result = result[item]
                return result
            return cls.__getitem__(self, key)
    return InnerCls