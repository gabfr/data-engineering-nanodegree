from airflow.plugins_manager import AirflowPlugin

import FactsCalculatorOperator
import HasRowsOperator
import S3ToRedshiftOperator


# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        FactsCalculatorOperator,
        HasRowsOperator,
        S3ToRedshiftOperator
    ]
