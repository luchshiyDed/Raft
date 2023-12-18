import socket
import socketserver
import sys
import pickle
import threading
import time
from typing import Union
import fastapi_jsonrpc as jsonrpc
from pydantic import BaseModel
from fastapi import Body
import requests
import json
import argparse
import Node
import main

app = jsonrpc.API()
parser = argparse.ArgumentParser()
parser.add_argument('self_ip', type=str)
args = parser.parse_args()

loc_ip = args.self_ip
own_loc_port = 9000
rport = 9001
api_v1 = jsonrpc.Entrypoint('/api/v1/jsonrpc')


class MyError(jsonrpc.BaseError):
    CODE = 5000
    MESSAGE = 'My error'

    class DataModel(BaseModel):
        details: str


class ownDataOutputModel(BaseModel):
    datas: float
    tag: str


class ownDataInputModel(BaseModel):
    tag: str = Body(..., examples=["square"])
    x: float = Body(..., examples=[0])


@api_v1.method(errors=[MyError])
def echo(
        in_params: ownDataInputModel
) -> ownDataOutputModel:
    global own_loc_port
    local_parametrs = in_params.dict()
    # print('in params', local_parametrs)
    loc_index = -1
    fnc_pow = lambda inp, p: inp ** p
    funcs = {"square": 2, "cubic": 3}

    tag = local_parametrs['tag']
    x = local_parametrs['x']

    if (tag not in funcs):
        tag = 'error'

    if tag == 'error':
        raise MyError(data={'details': 'error'})
    else:
        loc_index = fnc_pow(x, funcs[tag])
        return ownDataOutputModel(datas=loc_index, tag=tag + " my port is " + str(own_loc_port))


def call_rpc(func_name: str, input_model):
    global ip_port_for_rpc
    # print('rpc - ', ip_port_for_rpc)
    url = "http://" + ip_port_for_rpc + "/api/v1/jsonrpc"
    headers = {'content-type': 'application/json'}

    loc_json_rpc_req = {
        "jsonrpc": "2.0",
        "id": "0",
        "method": func_name,
        "params":
            {
                "in_params": input_model.dict()
            }
    }

    try:
        response = requests.post(url, data=json.dumps(loc_json_rpc_req), headers=headers, timeout=0.5)
    except Exception as err:
        print('rpc connection exception')
        return {'datas': 'error connection'}

    # print('res ', response.status_code)
    print('text ', response.text)
    if response.status_code == 200:
        response = response.json()

        if ('result' in response):
            return response['result']
        else:
            return {'datas': 'error fnc not found'}
    else:
        print('status code is not 200')
        return {'datas': 'error response'}


async def getResponce(add):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:

            sock.bind(add)
            sock.listen(1)
            conn, addr = sock.accept()
            with conn:
                print('Connected by', addr)
                data = conn.recv(1024)
                tries=0
                while data == b'' and tries>=0:
                    print(data)
                    data = conn.recv(1024)
                    tries-=1
                    time.sleep(0.3)
                return pickle.loads(data)
        except Exception as e:
            print(e)


@app.get("/rpc_old")
async def get(port: Union[int, None], func: Union[str, None] = 'heartbeat', key: Union[str, None] = 'mykey',
              params: Union[int, None] = None):
    address = (loc_ip, port)
    add = (loc_ip, rport)
    if params is not None:
        data = {
            'type': 'CR',
            'client': add,
            'request': f'{func} {key} {params}',
        }
        data.update({'request': f'{func} {key} {params}'})
    else:
        data = {
            'type': 'CR',
            'client': add,
            'request': f'{func} {key}',
        }
        if func=='lock' or func=='unlock':
            data.update({'request': f'{func} {key} {port}'})


    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.connect(address)
            sock.sendall(pickle.dumps(data, pickle.HIGHEST_PROTOCOL))
            res = await getResponce(add)
        except Exception as err:
            print(f'unable to connect {address} {err}')
            res='network error'

    return res


app.bind_entrypoint(api_v1)

if __name__ == '__main__':
    import uvicorn

    uvicorn.run('client:app', host=loc_ip, port=own_loc_port)
