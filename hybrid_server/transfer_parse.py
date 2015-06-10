import json
import scidb_parse
from plans import MyriaPlan

def parse_hybrid_transfers(queries, operator_type='impl_save', key='destination'):
    transfers = {}

    for query in queries:
        plan = MyriaPlan(query)
        for operator in plan.operators:
            if operator.type == operator_type:
                transfers[operator[key]] = plan.data['queryId']

    return transfers

if __name__ == "__main__":
    LOG_FILENAME = '/home/bhaynes/scidb.log'
    print json.dumps(parse_hybrid_transfers(scidb_parse.parse_plans(LOG_FILENAME)))