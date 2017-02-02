from pyspark.sql.types import * #StructField, StructType

class ColumnConfig:
    def __init__(self, name, path, cleaning_func, struct_type):
        self.name = name
        self.path = path
        self.cleaning_func = cleaning_func
        self.struct_type = struct_type

class DataFrameConfig:
    def __init__(self, sqlContext, col_configs):
        self.sqlContext = sqlContext
        self.columns = [ColumnConfig(*col) for col in col_configs]

    def toStructType(self):
        StructType(map(
            lambda col: StructField(col.name, col.struct_type, True),
            self.columns))

    def get_names(self):
        return map(lambda col: col.name, self.columns)

    def get_paths(self):
        return map(lambda col: col.path, self.columns)
