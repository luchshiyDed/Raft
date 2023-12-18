# Raft
runing Node: main ip port port1 port2 ...

running client: client ip

client uses 9000 and 9001 ports

requesting http://localhost:9000/rpc_old?port=8001&func=get&key=2&params=2

funcs ={get,set,pop,lock,unlock}

set(key,value) - sets a value for current key, if key already exist replaces value.

get(key) - returns the value for a key.

pop(key) - returns the value and deletes it.

lock(key) - locks by key

unlock(key) - unlocks by key version=port
