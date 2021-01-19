from json import load


class Config:
    def __init__(self, consensus_port, front_port, mempool_port):
        self.consensus_port = consensus_port
        self.front_port = front_port
        self.mempool_port = mempool_port

    @classmethod
    def read(cls, filename):
        with open(filename, 'r') as f:
            data = load(f)

        cls(
            data['ports']['consensus'],
            data['ports']['front'],
            data['ports']['mempool']    
        )