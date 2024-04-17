"""
SimpleRDD: This class acts as the backbone for distributed operations, mimicking the behavior of RDDs in Spark but without actual distribution capabilities.
SimpleDataFrame: Built on top of SimpleRDD, it allows operations on structured data like selection and filtering.
"""
class SimpleRDD:
    def __init__(self, data):
        self.data = data

    def map(self, func):
        # Map a function over each element in the RDD
        return SimpleRDD([func(x) for x in self.data])

    def filter(self, func):
        # Filter data based on a function
        return SimpleRDD([x for x in self.data if func(x)])

    def collect(self):
        # Return data as a list
        return self.data


class SimpleDataFrame:
    def __init__(self, data):
        # Expect data as a list of dictionaries, where each dictionary is a row
        self.rdd = SimpleRDD(data)

    def show(self):
        # Print out the data in the RDD
        print(self.rdd.collect())

    def select(self, *columns):
        # Select specific columns
        return SimpleDataFrame(self.rdd.map(lambda row: {col: row[col] for col in columns}).collect())

    def filter(self, condition):
        # Apply a filter condition
        return SimpleDataFrame(self.rdd.filter(condition).collect())

# Example usage
data = [
    {'name': 'Alice', 'age': 25, 'city': 'New York'},
    {'name': 'Bob', 'age': 30, 'city': 'Paris'},
    {'name': 'Charlie', 'age': 35, 'city': 'London'}
]

df = SimpleDataFrame(data)
df.show()  # Show original data

filtered_df = df.filter(lambda row: row['age'] > 25)
filtered_df.show()  # Show filtered data

selected_df = df.select('name', 'city')
selected_df.show()  # Show selected columns
