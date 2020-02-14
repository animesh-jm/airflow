# Airflow

Airflow是一個開源工作流管理平台(schedule workflow)，相較於其它ETL tools(e.g. Nifi, datastage, informatica, SSIS)，airflow並不負責處理資料(data flow)，airflow利用DAG來代表工作流程(workflow)，提供Web UI顯示有什麼工作、執行時間週期、工作之間的執行順序以及依賴關係。


## Quick start

    1.airflow needs a home, ~/airflow is the default
    (optional)
    $ export AIRFLOW_HOME=~/airflow
    
    2.install from pypi using pip
    $ pip install apache-airflow
    
    3.initialize the database
    $ airflow initdb
	
	4.start the scheduler
    $ airflow scheduler
	
	5.start the web server, default port is 8080
    $ airflow webserver -p 8080

![](images/webserver.png)

 - metadata db - sqlite
 - executor - SequentialExecutor
>   建議：executor換成**LocalExecutor**，metadata db換成**MySQL**或**Postgres**<br>

>   1. 修改airflow.cfg中executor<br>

    #The executor class that airflow should use. Choices include
    #SequentialExecutor, LocalExecutor, CeleryExecutor, DaskExecutor, KubernetesExecutor
    executor = LocalExecutor

>   2. 修改airflow.cfg，metadata database指向Postgres

    # The SqlAlchemy connection string to the metadata database.
    # SqlAlchemy supports many different database engine, more information
    # their website
    sql_alchemy_conn = postgresql+psycopg2://localhost/airflow

>   3.  open a PostgreSQL Shell

    psql

>   4.  create a new database

    CREATE DATABASE airflow

>   5.  初始化airflow

    airflow initdb


## DAG

**DAG - 有向無環圖（Directed Acyclic Graph）**

DAG 說明有什麼工作、執行時間週期、工作之間的執行順序以及依賴關係。<br>
DAG 最終目標將所有工作在對的時間依照上下游關係全部執行。

![](images/Unable.png)

## Task



## Operator



## Hook



## Sensor



## plugins


## 共用參數


## 動態 DAG、Task
>   **前端有10個table要備份資料至倉儲，某天需求多增加100個table**<br>
    ~~新增100個DAG或100個task~~

-   **動態task**<br>
[Creating a dynamic DAG using Apache Airflow]:<br> https://towardsdatascience.com/creating-a-dynamic-dag-using-apache-airflow-a7a6f3c434f3 <br>
[CREATE DYNAMIC WORKFLOW IN APACHE AIRFLOW]:<br> https://www.data-essential.com/create-dynamic-workflow-in-apache-airflow/

        tables = ['a','b','c','d','e','f','g']

        def dynamicTask(table, **kwargs):
            return DummyOperator(task_id="{0}".format(table),dag=dag)
        
        for table in tables:
            task1 >> dynamicTask(table) >> task3


    ![](images/dynamicTask.png)

-   **動態DAG**<br>
[Dynamically Generating DAGs in Airflow]: https://www.astronomer.io/guides/dynamically-generating-dags/

## xcom_push、xcom_pull
>   **DAG task之間value交換(subdag task、different DAGs and etc)**

-   **xcom讓task之間交換訊息, cross-communication的簡寫**

-   **task之間value交換**

        value = context['task_instance'].xcom_pull(task_ids='task1')

-   **subdag task之間value交換**

        value = context['task_instance'].xcom_pull(dag_id='sub_dag.xcom_subdag', task_ids='task1')

-   <font color=red size=4>任何執行過(含本身)</font> **DAG之間value的交換**

        get_xcom_class = XCom.get_many(
            execution_date=make_aware(datetime(2020, 2, 14)),
            dag_ids=["write_to_xcom"], 
            include_prior_dates=True)




>   **利用xcom_pull實作類似sensor效果**<br>
    -**DAG_B需要等DAG_A完成後才可以執行**

**A (DAG)**<br>
![](images/A_DAG.png)<br>
**B (DAG)**<br>
![](images/B_DAG.png)<br>

## SubDAG
[SubDAGs]<br>
https://airflow.apache.org/docs/stable/concepts.html?#subdags


## DAG模組化 ??
>   ***共用DAG***<br>
    -**任何DAG啟動前都必須檢查各式各樣的前置作業是否完成，將繁瑣前置作業包成DAG讓其它DAG import使用**

**precheck**

    import airflow
    from airflow.operators.python_operator import PythonOperator
    from airflow.operators.dummy_operator import DummyOperator

    class precheckDAG(object):

        def check(self,**context):
            return 'DONE'

        def precheck(self,parent_dag_name, child_dag_name, start_date, schedule_interval):
            dag = airflow.DAG(
                '%s.%s' % (parent_dag_name, child_dag_name),
                schedule_interval=schedule_interval,
                start_date=start_date,
            )

            task1 = DummyOperator(
                task_id='checktask1', 
                dag=dag
                )

            task2 = PythonOperator(
                task_id='checktask2', 
                python_callable=self.check, 
                dag=dag
                )        
            task1 >> task2

            return dag


**main (DAG)**

    from PrecheckDAG import precheckDAG

    precheck = precheckDAG()
    check = SubDagOperator(
        subdag = precheck.precheck('sub_dag', 'precheck_dag', date, interval),
        task_id='precheck_dag',
        dag=dag,
    )

    check >> task1 >> task3

![](images/precheck.png)


## airflow unittest
-  **IDE中模擬airflow scheduler執行DAG**
-  **單元測試**
