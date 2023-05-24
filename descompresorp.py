from algorithmp import HuffmanCoding
from mpi4py import MPI
import time
import sys 

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()


f1 = sys.argv[1]
h = HuffmanCoding(f1)

if rank == 0:
    Workers = size
    output_path = "descomprimidop-elmejorprofesor.txt"
    st = time.time()
    print("Decompressing")
    decompressed_text = b""
    chunk_size = 524288

    with open(f1, 'rb') as file, open(output_path, 'wb') as output:
        i = 1
        result_list=[]
        last_results=[]
        cont=True
        bit_string = ""
        while True:
            if cont:
                bit_string = ""
                byte_chunk = file.read(chunk_size)
                if not byte_chunk:
                    cont=False
                    bit_string=None
                else:
                    bit_string += "".join(format(byte, '08b') for byte in byte_chunk)
            else:
                bit_string=None

            if i < Workers:
                comm.send(bit_string, dest=i)
            else:
                status = MPI.Status()
                result = comm.recv(source=MPI.ANY_SOURCE, status=status)
                if result==None:
                    break
                if not cont:
                    last_results.append(result)
                else:
                    result_list.append(result)
                comm.send(bit_string, dest=status.Get_source())
            i += 1


        for a in range(1, Workers):
            result = comm.recv(source=MPI.ANY_SOURCE, status=status)
            if result!=None:
                last_results.append(result)
            comm.send(None, dest=a) 
        #print(result_list
        
        first=last_results.pop(0)
        last_results.append(first)

        for a in last_results:
            result_list.append(a)
        decompressed_text = b''.join(result_list)
        output.write(decompressed_text)
        end = time.time()
        total_t = end - st
        print("Decompressed")
        print("Decompression time: ", total_t) 

else:
    while True:            
        package = comm.recv(source=0)
        if package is None:
            comm.send(None, dest=0)
            break
        result = h.decompress(package)
        
        comm.send(result, dest=0)

comm.Barrier()
MPI.Finalize()
