import os 
import sys 
import filecmp

def main():
    f1 = sys.argv[1]
    input_filename, input_file_extension = os.path.splitext(f1)
    f2 = sys.argv[2]
    input_filename, input_file_extension = os.path.splitext(f2)

    if os.path.exists(f1) == False:
        print(f1 + " no existe")
        exit(0)
    elif os.path.exists(f2) == False:
        print(f2 + " no existe")
        exit(0)
    
    if filecmp.cmp(f1, f2):
        print("ok")
    else:
        print("nok")

if __name__ == "__main__":
    main()