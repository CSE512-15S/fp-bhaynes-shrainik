import re
from myria import *
from plans import MyriaPlan

class HybridPlanGenerator(object):
    def __init__(self, myria_url, scidb_plans, transfer_map):
        self.myria_url = myria_url
        self.scidb_plans = scidb_plans
        self.transfers = transfer_map

    def get_query(self, query_id):
        connection = MyriaConnection(rest_url=self.myria_url)
        plan = MyriaPlan(connection.get_query_status(query_id))

        for fragment in plan.fragments:
            fragment.data['system'] = 'Myria'

        for operator in plan.operators:
            if operator.type == 'FileScan' and 'scidb' in operator['source']['uri']:
                source = self._get_source_query(operator['source']['uri'])
                if source: self._connect_plans(source, (plan, operator))

        return plan

    def _get_source_query(self, uri, type='impl_save', destination_expression='transform_'):
        match = re.search('(.*/)+(?P<name>.*)$', uri)
        source_name = match.group('name') if match else None
        query_id = self.transfers[source_name] if source_name else None
        query = MyriaPlan(self.scidb_plans[int(query_id)]) if query_id else None
        operators = [op for op in (query.operators if query else [])
                        if op.type == type and re.search(destination_expression, op['destination'])]

        return (query, operators[0]) if operators else None

    @classmethod
    def _connect_plans(cls, (source_plan, source_operator), (destination_plan, destination_operator)):
        cls._get_merge_fragment_list(destination_plan).extend([f.data for f in source_plan.fragments])
        source_system = source_plan.fragments.next().data['system']
        destination_optype = source_system + 'Scan'
        destination_operator['opName'] = destination_operator['opName'].replace(destination_operator.type, destination_optype)
        destination_operator['opType'] = destination_optype
        destination_operator['source']['dataType'] = source_system
        destination_operator['argChild'] = source_operator.id

    @classmethod
    def _get_merge_fragment_list(cls, plan):
        if isinstance(plan, MyriaPlan):
            return cls._get_merge_fragment_list(plan.data)
        elif 'fragments' in plan:
            return plan['fragments']
        elif 'plans' in plan:
            return cls._get_merge_fragment_list(plan['plans'][0])
        elif 'plan' in plan:
            return cls._get_merge_fragment_list(plan['plan'])
