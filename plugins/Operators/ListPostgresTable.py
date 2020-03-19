import psycopg2
import logging
import airflow
from datetime import datetime, timedelta
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
from plugins.Operators.multiplyby5_operator import MultiplyBy5Operator
from airflow.operators.dummy_operator import DummyOperator

class ListTable(BaseOperator):

    ui_color = '#ffa0a0'

    @apply_defaults
    def __init__(
            self, 
            # dag_confing = None, 
            sql,
            postgres_conn_id,
            group_name,
            *args, **kwargs):
        super(ListTable, self).__init__(*args, **kwargs)
        # self.dag = dag_confing
        hook = PostgresHook(postgres_conn_id = postgres_conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        self.tiList = []

# // TODO: 動態產生不同Operator, 不指定產生特定Operator

        # generate dyncmic tasks
        for row in rows:
            self.tiList.append(MultiplyBy5Operator(task_id="{0}.{1}".format(group_name, row[0]), my_operator_param=row[0], dag=self.dag))
        
        # set task dependencies
        self.dag.task_dict[self.task_id].set_downstream(self.tiList)
        DummyOperator(task_id="{0}_{1}".format('done',group_name), dag=self.dag).set_upstream(self.tiList)

        cursor.close()

    def execute(self, **context):
        logging.info('------ generate_table_list ------')
