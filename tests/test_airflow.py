import pytest
import airflow
from datetime import datetime, timedelta
from airflow.sensors.sql_sensor import SqlSensor
from plugins.Operators import multiplyby5_operator
from airflow.operators.python_operator import PythonOperator

@pytest.fixture
def test_dag():
    """Airflow DAG for testing."""
    return airflow.DAG(
        "testdag",
        default_args={"owner": "airflow", "start_date": airflow.utils.dates.days_ago(0), 'provide_context': True},
        schedule_interval="@daily",
    )

class TestAirflowDAGTask:

    value = None

    dag = airflow.DAG(
        "testdag",
        default_args={"owner": "airflow", "start_date": airflow.utils.dates.days_ago(0), 'provide_context': True},
        schedule_interval="@daily",
    )   

    # run task
    def run_task(self,task, dag):
        dag.clear()
        task.run(
            start_date=dag.default_args["start_date"],
            end_date=dag.default_args["start_date"],
        )
    
    def xcom_value(self,**context):
        self.value = context['task_instance'].xcom_pull(task_ids='test_operator')
        print(self.value)
        return self.value

    
    def test_Operator(self,test_dag):

        sensor_task = SqlSensor(
            task_id = 'postgres_sensor',
            conn_id = 'postgres_dwh',
            sql = 'select * from staging.audit_runs limit 1;',
            poke_interval=3,
            timeout=5,
            dag=test_dag
        )

        test_operator = multiplyby5_operator.MultiplyBy5Operator(
            task_id='test_operator',
            my_operator_param=10,
            dag=test_dag
        )

        get_value = PythonOperator(
            task_id='get_value',
            python_callable=self.xcom_value,
            dag=test_dag
        )
        
        # sensor_task >> test_operator >> get_value
        self.run_task(sensor_task,test_dag)
        self.run_task(test_operator,test_dag)
        self.run_task(get_value,test_dag)


        assert str(self.value) == '50'

test = TestAirflowDAGTask()
test.test_Operator(test.dag)