class Node:

    def __init__(self, name, partition, cpus: int | None = None):

        self.name = name
        self.partition = partition
        self.cpus = cpus
    
    @staticmethod
    def from_json(path: str | Path):

       return 
