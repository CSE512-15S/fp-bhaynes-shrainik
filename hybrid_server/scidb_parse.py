import sys
import os
import re
import json
from collections import defaultdict
from datetime import datetime

class Log(object):
    OFFSET_FILE = 'last_offset'

    def __init__(self, filename, offset_filename=None):
        self.filename = filename
        self.offset_filename = offset_filename
        self.f = None
        self.is_eof = False

    def __enter__(self):
        self.f = open(self.filename, 'r')
        self.f.seek(self.get_last_offset())
        return self

    def __exit__(self, type, value, traceback):
        if self.f:
            self.put_last_offset(self.f.tell())
            self.f.close()

    def __iter__(self):
        while not self.is_eof:
            yield self.readline()
        raise StopIteration()

    def readline(self):
        position = self.f.tell()
        line = self.f.readline()
        self.is_eof = position == self.f.tell()
        return line

    def get_last_offset(self):
        if not self.offset_filename:
            return 0
        elif not os.path.exists(self.offset_filename):
            return 0
        with open(self.offset_filename, 'r+') as f:
            return int('0' + f.readline())

    def put_last_offset(self, offset):
        if self.offset_filename:
            with open(self.offset_filename, 'w') as f:
                f.write(str(offset))

class Parser(object):
    def __init__(self):
        pass

    def parse(self, statement, source):
        if not statement.startswith('2015'):
            date, thread_id = None, None
            tokens = statement.split(' ')
            body = statement
        else:
            tokens = statement.split(' ')
            date = datetime.strptime(' '.join([tokens.pop(0), tokens.pop(0)]), '%Y-%m-%d %H:%M:%S,%f')
            thread_id = tokens.pop(0).strip('[]')
            level = tokens.pop(0)
            body = ' '.join(tokens)

        if body.startswith('Initialized query'):
            return InitializedQueryStatement(date, thread_id, tokens)
        elif body.startswith('Parsing query'):
            return ParsedQueryStatement(date, thread_id, tokens)
        elif body.startswith('The physical plan is detected'):
            return PhysicalQueryPlan(date, thread_id, tokens, source)
        elif 'is being committed' in body:
            return CommittedQueryStatement(date, thread_id, tokens)
        else:
            return UnknownStatement(date, thread_id, tokens)



    def _parse_body(self, tokens):
        return ' '.join(tokens)

class Statement(object):
    def __init__(self, date, thread_id, tokens):
        self.date = date
        self.thread_id = thread_id
        self.tokens = tokens
        self.body = ' '.join(tokens)

class QueryStatement(Statement):
    SCIDB_OFFSET = int(1E6)

    queries = defaultdict(list)
    current_query = None

    def __init__(self, date, thread_id, tokens, query_id):
        super(QueryStatement, self).__init__(date, thread_id, tokens)
        self.query_id = query_id
        QueryStatement.current_query = query_id
        QueryStatement.queries[query_id].append(self)

    @property
    def start_time(self):
        statements = self._statements(InitializedQueryStatement)
        return statements[0].date if statements else None

    @property
    def end_time(self):
        statements = self._statements(CommittedQueryStatement)
        return statements[0].date if statements else None

    @property
    def query_text(self):
        statements = self._statements(ParsedQueryStatement)
        return statements[0]._query_text if statements else None

    @property
    def operators(self):
        statements = self._statements(PhysicalQueryPlan)
        return statements[0]._operators() if statements else None

    def to_dict(self):
        return {
            'url': None,
            'queryId': int(self.query_id),
            'subQueryId': 0,
            'langugage': 'AFL',

            'startTime': self.start_time.isoformat() if self.start_time else None,
            'finishTime': self.end_time.isoformat() if self.end_time else None,
            'elapsedNanos': int(((self.end_time or datetime.min) - (self.start_time or datetime.min)).total_seconds() * 1E9),
            'submitTime': (self.start_time or datetime.min).isoformat(),

            'logicalRa': self.query_text,
            'rawQuery': self.query_text,
            'message': None,
            'status': 'SUCCESS',
            'profilingMode': [],
            'ftMode': '',

            'plan': {
                'type': 'SubQuery',
                'fragments': [{
                    'system': 'SciDB',
                    'fragmentIndex': self.SCIDB_OFFSET,
                    'overrideWorkers': None,
                    'operators': [dict({
                        'opId': op.id + self.SCIDB_OFFSET,
                        'opName': op.name,
                        'opType': op.name,
                        'argChild': op.children[0].id if op.children else None
                    }, **op.metadata) for op in self.operators or []]
                }]
            }
        }

    def _statements(self, type):
        return [s for s in self.queries[self.query_id] if isinstance(s, type)]

class InitializedQueryStatement(QueryStatement):
    def __init__(self, date, thread_id, tokens):
        super(InitializedQueryStatement, self).__init__(date, thread_id, tokens, tokens[2].strip('()\n'))

class ParsedQueryStatement(QueryStatement):
    def __init__(self, date, thread_id, tokens):
        super(ParsedQueryStatement, self).__init__(date, thread_id, tokens, tokens[1].strip('query():'))
        self._query_text = ' '.join(tokens[2:])

class CommittedQueryStatement(QueryStatement):
    def __init__(self, date, thread_id, tokens):
        super(CommittedQueryStatement, self).__init__(date, thread_id, tokens, tokens[1].strip('query():'))

class PhysicalQueryPlan(QueryStatement):
    def __init__(self, date, thread_id, tokens, source):
        super(PhysicalQueryPlan, self).__init__(date, thread_id, tokens, QueryStatement.current_query)
        self.plan = self._get_plan(source)

    def _operators(self):
        operators = []
        for id, (level, name, line) in enumerate([(len(line) - len(line.strip('>')), line.split(' ')[1], line) for line in self.plan if '[pNode]' in line]):
            parent = [op for op in operators if op.level == level - 1][-1] if operators else None
            operators.append(Operator(id, name, level, self, ' '.split(line), self.plan[id+2:], parent))

        for operator in operators:
            operator.children = [op for op in operators if op.parent == operator]

        return operators

    def _get_plan(self, source):
        plan = []
        for line in source:
            if 'Single executing' in line:
                break
            elif not 'DEBUG' in line:
                plan.append(line.strip())
        return plan

class EmptyStatement(Statement):
    def __init__(self, date, thread_id, tokens):
        super(EmptyStatement, self).__init__(date, thread_id, tokens)

class UnknownStatement(Statement):
    def __init__(self, date, thread_id, tokens):
        super(UnknownStatement, self).__init__(date, thread_id, tokens)

class Operator(object):
    def __init__(self, id, name, level, statement, tokens, detail, parent, children=None):
        self.id = id
        self.name = name
        self.level = level
        self.statement = statement
        self.tokens = tokens
        self.parent = parent
        self.children = children

        if self.name == 'impl_save':
            self.metadata = {'destination': re.search('schema (?P<name>[^<]+)', detail[0]).group('name')}
        else:
            self.metadata = {}

def plan_map(filename):
    queries = {}
    for plan in parse_plans(filename):
        queries[int(plan.query_id)] = plan.to_dict()
    return queries

def parse_plans(filename):
    parser = Parser()
    with Log(filename) as log:
        for line in log:
            statement = parser.parse(line, log)
            if isinstance(statement, CommittedQueryStatement):
                 yield statement

if __name__ == "__main__":
    for plan in parse_plans('/home/bhaynes/scidb.log'):
        print json.dumps(plan.to_dict())
