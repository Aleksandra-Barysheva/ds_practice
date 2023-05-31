# Since the needs for data transformation are likely to change,
# guided by the Open-closed SOLID principle
# we will create a separate Transform class, in the heirs of
# which we will define all the transformation logic and pass an object
# of this class to ETL without changing the logic of ETL itself.

class Transform:
    def __call__(self, data):
        data = self.apply_map(data)
        data = self.apply_filter(data)
        data = self.apply_reduce(data)
        return data

    def apply_map(self, data):
        raise NotImplementedError

    def apply_filter(self, data):
        raise NotImplementedError

    def apply_reduce(self, data):
        raise NotImplementedError
