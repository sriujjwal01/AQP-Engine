import math
import random
import hashlib

class CountMinSketch:
    def __init__(self, width: int, depth: int, seed: int | None = None):
        self.w = width
        self.d = depth
        self.tables = [[0]*width for _ in range(depth)]
        self.rand = random.Random(seed)
        
        self.salts = [self.rand.getrandbits(64).to_bytes(8,'little') for _ in range(depth)]

    def _hash(self, x, i):
        h = hashlib.blake2b(digest_size=8, person=self.salts[i])
        h.update(str(x).encode())
        return int.from_bytes(h.digest(), 'little') % self.w

    def add(self, x, c=1):
        for i in range(self.d):
            idx = self._hash(x, i)
            self.tables[i][idx] += c

    def query(self, x):
        return min(self.tables[i][self._hash(x,i)] for i in range(self.d))

class TinyHLL:
    
        self.m = m
        self.registers = [0]*m

    def _hash(self, x):
        return int.from_bytes(hashlib.blake2b(str(x).encode(), digest_size=8).digest(),'little')

    def add(self, x):
        h = self._hash(x)
        idx = h % self.m
        w = h >> 7
        rho = (w.bit_length() - (w ^ (1 << (w.bit_length()-1))).bit_length()) if w != 0 else 1
        self.registers[idx] = max(self.registers[idx], rho)

    def estimate(self):
        alpha = 0.7213/(1 + 1.079/self.m)
        Z = sum(2.0**(-r) for r in self.registers)
        return alpha * self.m * self.m / Z
