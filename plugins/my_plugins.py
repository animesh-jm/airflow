from airflow.plugins_manager import AirflowPlugin

from plugins.Operators.multiplyby5_operator import MultiplyBy5Operator
from plugins.Operators.dwh_operators import AuditOperator,PostgresToPostgresOperator
from plugins.Operators.ListPostgresTable import ListTable


class MyPlugin(AirflowPlugin):
    name = "myplugin"
    operators = [MultiplyBy5Operator,AuditOperator,PostgresToPostgresOperator,ListTable]
    # A list of class(es) derived from BaseHook
    hooks = []
    # A list of class(es) derived from BaseExecutor
    executors = []
    # A list of references to inject into the macros namespace
    macros = []
    # A list of objects created from a class derived
    # from flask_admin.BaseView
    admin_views = []
    # A list of Blueprint object created from flask.Blueprint
    flask_blueprints = []
    # A list of menu links (flask_admin.base.MenuLink)
    menu_links = []