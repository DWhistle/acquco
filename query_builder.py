
from enum import Enum

class QueryElement:
    def to_query(self) -> str:
        raise NotImplementedError(self)

class Metric(QueryElement):
    def __init__(self, metrics: str) -> None:
        # could be abstracted and validated somehow
        self.metrics = metrics
    def to_query(self) -> str:
        return self.metrics


class Table(Enum):
   ORDER = 'FacebookAdOrders'
   DATA = 'FacebookAdData'

class Granularity(QueryElement):
    #applicable for validation
    available_granularities = {
        Table.ORDER : {'monthly', 'daily'},
        Table.DATA : {'monthly', 'daily', 'weekly'}
    }
    def __init__(self, granularity: str) -> None:
        self.granularity = granularity

    def to_query(self) -> str:
        return self.granularity

class Condition(QueryElement):
    condition_fields = {'account_id', 'ad_group_id'}

    def __init__(self, field: str, value: int):
        assert(field in self.__class__.condition_fields)
        self.field = field
        self.value = value

    def to_query(self) -> str:
        return f"""
        {self.field} = {self.value}
        """

class QueryBuilder:
    def _source(self) -> str:
        raise NotImplementedError()

    def with_metric(self, metric: Metric):
        self.metric = metric
        return self

    def with_condition(self, condition: Condition):
        self.condition = condition
        return self

    def build(self):
        return f"""
        SELECT {self.metric.to_query()}
        FROM {self._source()}
        WHERE {self.condition.to_query()}
        """

class SimpleQueryBuilder(QueryBuilder):

    def with_granularity(self, granularity: Granularity):
        self.granularity = granularity
        return self

    def with_table(self, table: Table):
        self.table = table
        return self
    
    def _source(self):
        return f"{self.table.value}_{self.granularity.to_query()}"

class JoinOperation:
    def __init__(self, table1: Table, table2: Table, field1: str, field2: str, type: str) -> None:
        #possibly validation
        self.table1 = table1
        self.table2 = table2
        self.field1 = field1
        self.field2 = field2
        self.type = type

### possible evolution
class MultiTableQueryBuilder(QueryBuilder):

    def with_source(self, table: Table, granularity: Granularity, joins: list[JoinOperation]):
        granularity = granularity.to_query()
        def join_to_str(join: JoinOperation):
            return f"""
            {join.table1}_{granularity} ON {join.table1}_{granularity}.{join.field1} = {join.table2}_{granularity}.{join.field2}
            """
        self._source = f"{table.value}_{granularity}" \
        #could be more join types
        + "JOIN {}".join(map(join_to_str, joins))

    def _source(self):
        return self._source


def example():
    metrics = Metric("Sum(clicks)")
    granularity = Granularity("Weekly")
    cond = Condition("account_id", 20)
    print("### Simple Query ###")
    print(SimpleQueryBuilder()
    .with_table(Table.DATA)
    .with_condition(cond)
    .with_granularity(granularity)
    .with_metric(metrics)
    .build())
    
if __name__ == "__main__":
    example()