#!/usr/bin/env python
import sys
import SimpleHTTPServer
import SocketServer
import json
import re
import argparse
import transfer_parse
import scidb_parse
from hybrid_plans import HybridPlanGenerator

class HybridTCPServer(SocketServer.TCPServer):
    def __init__(self, address, handler, myria_url, scidb_log):
        SocketServer.TCPServer.__init__(self, address, handler)
        self.scidb_plans = scidb_parse.plan_map(scidb_log)
        self.hybrid_plans = HybridPlanGenerator(
            myria_url,
            self.scidb_plans,
            transfer_parse.parse_hybrid_transfers(self.scidb_plans.values()))


class HybridPlanHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
    QUERY_PATTERN = r'query/query-(?P<id>\d+)$'

    def do_GET(self):
        match = re.search(self.QUERY_PATTERN, self.path)
        if match:
            self.get_hybrid_query(int(match.group('id')))
        else:
            self.send_response(404)

    def get_hybrid_query(self, query_id):
        self.send_response(200 if query_id else 404)
        self.send_header("Content-type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

        self.wfile.write(json.dumps(self.server.hybrid_plans.get_query(query_id, flatten=True).data))


def parse_arguments(arguments):
    parser = argparse.ArgumentParser(description='Launch webserver that serves hybrid plans')
    parser.add_argument('myria_url', type=str, help='REST URL for Myria')
    parser.add_argument('scidb_log', type=str, help='Path to SciDB log filename')

    parser.add_argument('--port', type=int, default=8752, help='Webserver port number')

    return parser.parse_args(arguments)


if __name__ == "__main__":
    arguments = parse_arguments(sys.argv[1:])

    HybridTCPServer(('0.0.0.0', arguments.port),
                    HybridPlanHandler,
                    arguments.myria_url,
                    arguments.scidb_log).serve_forever()