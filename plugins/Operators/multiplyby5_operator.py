import logging
from airflow.models import BaseOperator
from datetime import datetime
from airflow.utils.decorators import apply_defaults

class MultiplyBy5Operator(BaseOperator):

    ui_color = '#b9f2ff'

    @apply_defaults
    def __init__(self, my_operator_param, *args, **kwargs):
        self.operator_param = my_operator_param
        super(MultiplyBy5Operator, self).__init__(*args, **kwargs)

    def execute(self, **context):
        logging.info('------ operator_param ------: %s', self.operator_param + '######')
        return (self.operator_param + '5')
        # return (self.operator_param * 5)