"""
TCP simple healthcheck for long-running mode (e.g. backup).
"""

import threading
import SocketServer


class ThreadedTCPRequestHandler(SocketServer.BaseRequestHandler):

    def handle(self):
        # just close the connection
        pass


class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass


def start(host, port):
    """Starts TCP simple healthcheck on given host/port in
    a separate port.
    """
    server = ThreadedTCPServer((host, port), ThreadedTCPRequestHandler)

    # Start a thread with the server -- that thread will then start one
    # more thread for each request
    server_thread = threading.Thread(target=server.serve_forever)
    # Exit the server thread when the main thread terminates
    server_thread.daemon = True
    server_thread.start()
