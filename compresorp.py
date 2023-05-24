from algorithmp import HuffmanCoding
from mpi4py import MPI
import time
import sys 


comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

f1 = sys.argv[1]
h = HuffmanCoding(f1)

if rank==0:
    print("Compressing")
    st = time.time()
    h.saveDict()    
    
    Workers = size
    output_path = "comprimidop.elmejorprofesor"
    compressed_text = b""
    chunk_size = 524288

    with open(f1, 'rb') as file, open(output_path, 'wb') as output:
        i = 1
        result_list=[]
        last_results=[]
        while True:
            byte_chunk = file.read(chunk_size)
            if not byte_chunk:
                break

            if i < Workers:
                comm.send(byte_chunk, dest=i)
            else:
                result = comm.recv(source=((i-1)%(Workers-1))+1)
                result_list.append(result)
                comm.send(byte_chunk, dest=((i-1)%(Workers-1))+1)
            i += 1
        

        for a in range(1, Workers):
            result = comm.recv(source=a)
            if result!=None:
                last_results.append(result)
            comm.send(None, dest=a) 
        #print(result_list)
        
       
        
        first=last_results.pop(0)
        last_results.append(first)

       
        for a in last_results:
            result_list.append(a)
        compressed_text=b''.join(result_list)
        output.write(compressed_text)
    
    
    

    end = time.time()
    total_t = end - st
    print("Compression time: ", total_t)
else:
    while True:            
            package = comm.recv(source=0)
            if package is None:
                break
            result = h.compress(package)
            comm.send(result, dest=0)
