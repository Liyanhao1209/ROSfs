import struct

backend = "/data/GroundAir/Edge/172.17.0.2.bag"
brick_name = 1

def read_uint32(f):
    return unpack_uint32(f.read(4))

def unpack_uint32(v):
    return struct.unpack('<L',v)[0]

def read_sized(f):
    size = read_uint32(f)
    print(size)

if __name__ == "__main__":
    data_brick = f"{backend}/{brick_name}.brick"
    
    dbf = open(data_brick,'rb')
    dbf.seek(102)
    read_sized(dbf)
    