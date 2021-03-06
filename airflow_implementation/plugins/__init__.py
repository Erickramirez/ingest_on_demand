from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class GeneralPlugin(AirflowPlugin):
    name = "general_plugin"
    operators = [operators.DataQualityOperator, operators.LoadCSVFromDriveOperator]
    helpers = [helpers.DMLScripts]
