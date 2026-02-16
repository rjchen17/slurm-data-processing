class Node:

    def __init__(self, name, partition, cpus: int | None = None):

        self.name = name
        self.partition = partition
        self.cpus = cpus
    
    def __str__(self):
        return f"Node {self.name} on partition {self.partition} with {self.cpus} cpus. "
