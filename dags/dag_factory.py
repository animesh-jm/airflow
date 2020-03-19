# import airflow
from airflow import DAG
from plugins.dagfactory import DagFactory 
from airflow.operators.bash_operator import BashOperator

config_file = "/Users/chuhaoyu/airflow/dags/dag_factory.yml"

example_dag_factory = DagFactory(config_file)
dag, tasks = example_dag_factory.generate_dags(globals())

# // FIXME: ymal和py檔中task要混用的話, 需在py檔中寫順序

# task1 = BashOperator(
#         task_id='task1',
#         bash_command='sleep 5',
#         dag = dag
#         )   

# [tasks['do_something'],tasks['task3']] >> task1 >> tasks['generate_order_table_list']
