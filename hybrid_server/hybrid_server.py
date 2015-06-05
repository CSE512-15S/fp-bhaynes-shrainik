#!/usr/bin/env python
import sys
from collections import defaultdict
import SimpleHTTPServer
import SocketServer
import itertools
import json
import re
import urllib
import urlparse
import argparse
from dateutil.parser import parse
import logging
import transfer_parse
import scidb_parse
from hybrid_plans import HybridPlanGenerator

class HybridTCPServer(SocketServer.TCPServer):
    def __init__(self, address, handler, myria_url, scidb_log, scidb_workers, transfer_path):
        SocketServer.TCPServer.__init__(self, address, handler)

        self.myria_url = myria_url
        self.logger = logging.getLogger('HybridTCPServer')
        self.start_times = {}

        self.logger.debug('Beginning SciDB log parse')
        self.scidb_workers = scidb_workers
        self.scidb_plans, self.scidb_profiling = scidb_parse.plan_map(scidb_log, scidb_workers, transfer_path)

        self.logger.debug('Beginning Hybrid plan generation')
        self.hybrid_plans = HybridPlanGenerator(
            myria_url,
            self.scidb_plans,
            transfer_parse.parse_hybrid_transfers(self.scidb_plans.values()))

        self.logger.debug('Initialization complete')


class HybridPlanHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
    QUERY_PATTERN = r'query/query-(?P<id>\d+)$'
    PROFILE_PATH = r'logs/profiling'
    RANGE_PATH = r'logs/range'
    AGGREGATED_PATH = r'logs/aggregated_sent'
    HISTOGRAM_PATH = r'histogram'

    def do_GET(self):
        match = re.search(self.QUERY_PATTERN, self.path)
        if match:
            self.get_hybrid_query(int(match.group('id')))
        elif re.search(self.PROFILE_PATH, self.path) is not None:
            self.get_hybrid_profile(*self._extract_profiling_arguments(self.path))
        elif re.search(self.AGGREGATED_PATH, self.path) is not None:
            self.get_hybrid_aggregated_sent(*self._extract_aggregated_sent_arguments(self.path))
        elif re.search(self.HISTOGRAM_PATH, self.path) is not None:
            self.get_hybrid_histogram(*self._extract_histogram_arguments(self.path))
        elif re.search(self.RANGE_PATH, self.path) is not None:
            self.get_hybrid_range(*self._extract_range_arguments(self.path))
        else:
            self.send_response(404)

    def get_hybrid_query(self, query_id):
        self.send_response(200 if query_id else 404)
        self.send_access_control_headers()
        self.send_header("Content-type", "application/json")
        self.end_headers()

        self.wfile.write(json.dumps(self.server.hybrid_plans.get_query(query_id, flatten=True).data))

    def get_hybrid_profile(self, system, query_id, subquery_id, fragment_id, start_time, end_time, only_root, minimum_length):
        if system == 'Myria':
            self.send_response(301)
            self.send_access_control_headers()
            self.send_header('Location', self._create_myria_url(self.PROFILE_PATH,
                                                                queryId=query_id,
                                                                subqueryId=subquery_id,
                                                                fragmentId=fragment_id,
                                                                start=start_time,
                                                                end=end_time,
                                                                onlyRootOperator=only_root,
                                                                minimumLength=minimum_length))
            self.end_headers()
        elif system == 'SciDB' and subquery_id:
            self.send_response(200)
            self.send_access_control_headers()
            self.send_header("Content-type", "application/json")
            self.end_headers()

            self.wfile.write('workerId,opId,startTime,endTime,numTuples\n')
            profile = self.server.scidb_profiling[int(subquery_id)]
            for shuffle in profile.shuffles:
                shuffle_start = int((shuffle.date     - shuffle.start_time).total_seconds() * 1E9)
                shuffle_end =   int((profile.end_time - shuffle.start_time).total_seconds() * 1E9)

                if start_time <= shuffle_start and end_time > shuffle_start or \
                   start_time < shuffle_end    and end_time > shuffle_end:
                    self.wfile.write('{workerId},{opId},{startTime},{endTime},{numTuples}\n'.format(
                                     workerId=shuffle.worker_id,
                                     opId=shuffle.operator.id,
                                     startTime=shuffle_start,
                                     endTime= shuffle_end,
                                     numTuples=shuffle.cardinality))
        else:
            self.send_response(404)
            self.send_access_control_headers()
            self.end_headers()

    def get_hybrid_aggregated_sent(self, system, query_id, subquery_id, fragment_id):
        if system == 'Myria':
            self.send_response(301)
            self.send_access_control_headers()
            self.send_header('Location', self._create_myria_profiling_url(self.PROFILE_PATH, queryId=query_id, subqueryId=subquery_id))
            self.end_headers()
        elif system == 'SciDB' and subquery_id:
            self.send_response(200)
            self.send_access_control_headers()
            self.end_headers()

            self.wfile.write('fragmentId,numTuples,duration\n')
            profile = self.server.scidb_profiling[int(subquery_id)]
            cardinality = sum([shuffle.cardinality for shuffle in profile.shuffles])
            duration = int((profile.end_time - profile.shuffles[0].start_time).total_seconds() * 1E9)

            self.wfile.write('{fragmentId},{numTuples},{duration}\n'.format(
                             fragmentId=fragment_id,
                             numTuples=cardinality,
                             duration=duration))
        else:
            self.end_headers()
            self.send_response(404)

    def get_hybrid_histogram(self, system, query_id, subquery_id, fragment_id, start_time, end_time, only_root, step_size):
        if system == 'Myria':
            self.send_response(301)
            self.send_access_control_headers()
            self.send_header('Location', self._create_myria_url(self.HISTOGRAM_PATH,
                                                                queryId=query_id,
                                                                subqueryId=subquery_id,
                                                                fragmentId=fragment_id,
                                                                start=start_time,
                                                                end=end_time,
                                                                onlyRootOperator=only_root,
                                                                step=step_size))
            self.end_headers()
        elif system == 'SciDB' and subquery_id:
            self.send_response(200)
            self.send_access_control_headers()
            self.end_headers()

            self.wfile.write('opId,nanoTime,numWorkers\n')
            profile = self.server.scidb_profiling[int(subquery_id)]
            bins = xrange(start_time, end_time, step_size)
            histogram = defaultdict(int)

            for shuffle in profile.shuffles:
                shuffle_start = int((shuffle.date     - shuffle.start_time).total_seconds() * 1E9)
                shuffle_end =   int((profile.end_time - shuffle.start_time).total_seconds() * 1E9)
                for bin_start in bins:
                    bin_end = bin_start + step_size
                    if bin_start <= shuffle_start and bin_end > shuffle_start or \
                       bin_start < shuffle_end    and bin_end > shuffle_end:
                       histogram[(shuffle.operator.id, bin_start)] += 1

            print histogram
            for (operator_id, bin_start), workers in histogram.items():
                self.wfile.write('{},{},{}\n'.format(operator_id, bin_start, workers))

        else:
            self.send_response(404)
            self.send_access_control_headers()
            self.end_headers()

    def get_hybrid_range(self, system, query_id, subquery_id, fragment_id):
        if system == 'Myria':
            self.send_response(301)
            self.send_access_control_headers()
            self.send_header('Location', self._create_myria_url(self.RANGE_PATH,
                                                                queryId=query_id,
                                                                subqueryId=subquery_id,
                                                                fragmentId=fragment_id))
            self.end_headers()
        elif system == 'SciDB' and subquery_id:
            self.send_response(200)
            self.send_access_control_headers()
            self.end_headers()

            if query_id not in self.server.start_times:
                self.server.start_times[query_id] = parse(self.server.hybrid_plans.get_query(query_id).data['startTime']).replace(tzinfo = None)
            profile = self.server.scidb_profiling[int(subquery_id)]

            self.wfile.write('min_startTime,max_endTime\n')
            self.wfile.write('{},{}\n'.format(
                max(int((profile.plan.start_time - self.server.start_times[query_id]).total_seconds() * 1E9), 0),
                max(int((profile.plan.end_time - self.server.start_times[query_id]).total_seconds() * 1E9), 0)))
        else:
            self.send_response(404)
            self.send_access_control_headers()
            self.end_headers()

#nocatgw.cs.washington.edu - - [05/Jun/2015 21:20:05] "GET /logs/contribution?queryId=5099&subqueryId=0&fragmentId=9 HTTP/1.1" 404 -

    def _create_myria_url(self, path, **kwargs):
        querystring = '&'.join(['='.join(map(urllib.quote_plus, map(str, pair))) for pair in kwargs.items()])
        # Should use urlparse, but am being lazy...
        return '{}/{}?{}'.format(self.server.myria_url, path, querystring)

    def send_access_control_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    @staticmethod
    def _extract_profiling_arguments(path):
        url = urlparse.urlparse(path)
        querystring = urlparse.parse_qs(url.query)
        return (querystring.get('system', [None])[0],
                int(querystring.get('queryId', [0])[0]),
                int(querystring.get('subqueryId', [0])[0]),
                int(querystring.get('fragmentId', [0])[0]),
                int(querystring.get('start', [0])[0]),
                int(querystring.get('end', [1E20])[0]),
                querystring.get('onlyRootOp', [None])[0],
                int(querystring.get('minLength', [0])[0]))

    @staticmethod
    def _extract_aggregated_sent_arguments(path):
        url = urlparse.urlparse(path)
        querystring = urlparse.parse_qs(url.query)
        return (querystring.get('system', [None])[0],
                int(querystring.get('queryId', [0])[0]),
                int(querystring.get('subqueryId', [0])[0]),
                querystring.get('fragmentId', [-1])[0])

    @staticmethod
    def _extract_histogram_arguments(path):
        url = urlparse.urlparse(path)
        querystring = urlparse.parse_qs(url.query)
        return (querystring.get('system', [None])[0],
                int(querystring.get('queryId', [0])[0]),
                int(querystring.get('subqueryId', [0])[0]),
                querystring.get('fragmentId', [-1])[0],
                int(querystring.get('start', [0])[0]),
                int(querystring.get('end', [0])[0]),
                querystring.get('onlyRootOp', [None])[0],
                int(querystring.get('step', [0])[0]))

    @staticmethod
    def _extract_range_arguments(path):
        url = urlparse.urlparse(path)
        querystring = urlparse.parse_qs(url.query)
        return (querystring.get('system', [None])[0],
                int(querystring.get('queryId', [0])[0]),
                int(querystring.get('subqueryId', [0])[0]),
                querystring.get('fragmentId', [-1])[0])

def parse_arguments(arguments):
    parser = argparse.ArgumentParser(description='Launch webserver that serves hybrid plans')
    parser.add_argument('myria_url', type=str, help='REST URL for Myria')
    parser.add_argument('scidb_log', type=str, help='Path to SciDB log filename')
    parser.add_argument('scidb_workers', type=int, help='Total number of SciDB workers')
    parser.add_argument('transfer_path', type=str, help='Path for hybrid transfer intermediate data')

    parser.add_argument('--port', type=int, default=8752, help='Webserver port number')

    return parser.parse_args(arguments)


if __name__ == "__main__":
    arguments = parse_arguments(sys.argv[1:])

    logging.getLogger().setLevel(logging.DEBUG)

    HybridTCPServer(('0.0.0.0', arguments.port),
                    HybridPlanHandler,
                    arguments.myria_url,
                    arguments.scidb_log,
                    arguments.scidb_workers,
                    arguments.transfer_path).serve_forever()