import random

"""
Fault Tolerance: Tasks will retry up to a specified limit when they fail. If all retries fail, the task returns an empty result.
Data Shuffling: After the map tasks, we simulate shuffling data based on a key. The shuffled data forms new partitions that feed into the reduce tasks, mimicking the map-reduce pattern of Spark.
"""

class Task:
    def __init__(self, function, partition, retry_limit=1):
        self.function = function
        self.partition = partition
        self.retry_limit = retry_limit
        self.retry_count = 0

    def execute(self):
        try:
            # Introducing a failure scenario randomly for demonstration
            if random.random() < 0.1:  # 10% chance to fail
                raise Exception("Simulated task failure")
            return [self.function(item) for item in self.partition]
        except Exception as e:
            if self.retry_count < self.retry_limit:
                self.retry_count += 1
                print(f"Retrying task {id(self)}, attempt {self.retry_count}")
                return self.execute()
            else:
                print(f"Task {id(self)} failed after {self.retry_limit} retries")
                return []  # Returning empty on failure

def shuffle_data(data):
    """ Shuffle data simulates the shuffling of data between map and reduce stages """
    shuffled = {}
    for item in data:
        key = item[0]  # Assuming the key is the first element in a tuple
        if key not in shuffled:
            shuffled[key] = []
        shuffled[key].append(item)
    return list(shuffled.values())

class Stage:
    def __init__(self, tasks, shuffle_before=False):
        self.tasks = tasks
        self.shuffle_before = shuffle_before

    def execute(self):
        if self.shuffle_before:
            # Gather all results from previous stage and shuffle
            intermediate_results = []
            for task in self.tasks:
                intermediate_results.extend(task.execute())
            new_partitions = shuffle_data(intermediate_results)
            # Creating new tasks from shuffled data for this stage
            new_tasks = [Task(lambda x: x, partition) for partition in new_partitions]
            self.tasks = new_tasks

        results = []
        for task in self.tasks:
            task_results = task.execute()
            results.extend(task_results)
        return results



"""
Execute the above classes via a demo
"""
def map_function(x):
    return (x % 2, x)  # Return key-value pair, key is x % 2

def reduce_function(values):
    return (values[0][0], sum([item[1] for item in values]))  # Sum values by key

# Simulating partitions of data
data_partitions = [[i for i in range(1, 6)], [i for i in range(6, 11)]]

# Creating map tasks
map_tasks = [Task(map_function, partition) for partition in data_partitions]

# Creating reduce tasks, initially just dummy tasks that will be replaced post-shuffling
reduce_tasks = [Task(reduce_function, []) for _ in range(2)]  # 2 partitions for simplification

# Create two stages: one for map and one for reduce
map_stage = Stage(map_tasks)
reduce_stage = Stage(reduce_tasks, shuffle_before=True)

# Create a job with both stages
job = Job([map_stage, reduce_stage])

# Execute the job
job_results = job.execute()
print("Final Job Results:")
print(job_results)

