from algorithm import HuffmanCoding
import time
import sys 

f1 = sys.argv[1]
path = "LaBiblia.txt"
h = HuffmanCoding(f1)

print("Compressing")
st = time.time()
output_path = h.compress()
end = time.time()

total_t = end - st

print("Compression time: ", total_t)