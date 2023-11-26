import pickle
import socketserver
import sys

import Node


def start():
    adresses = {
    }
    me=int(sys.argv[2])
    for i in sys.argv[2:]:
        adresses.update({int(i): (sys.argv[1], int(i))})
    node = Node.node(me, adresses=adresses)

    class Handler(socketserver.StreamRequestHandler):
        def handle(self):
            self.data = self.request.recv(1024)
            node.handle(self.data,self.request)

    with socketserver.TCPServer((sys.argv[1],me), Handler) as server:
        server.serve_forever()
if __name__ == '__main__':
    start()
