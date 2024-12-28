import ray
import time

# Initialize Ray
ray.init(address="auto")  # Automatically connect to the cluster


# Define a remote function
@ray.remote
def hello_world():
    time.sleep(1)  # Simulate some work
    return "Hello World from Ray!"


# Submit the task to the cluster
futures = [hello_world.remote() for _ in range(10)]  # Submit 10 tasks

# Wait for the tasks to finish and collect results
results = ray.get(futures)

# Print the results
for result in results:
    print(result)
