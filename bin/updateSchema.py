"""this would backup schema tables and views"""
import os
import sys

PROJ_DIR = os.path.dirname(os.path.abspath(os.path.join(__file__, '..', '..')))
sys.path.append(PROJ_DIR)

import datetime
import yaml
import subprocess
import shutil
import pandas as pd
from mikesnowflake.access.snowFlakeAccess import SnowFlakeAccess
from mikesnowflake.util.yamlUtil import getYamlDependencies

os.nice(20)


def main(user, password):
    """the main entry point to the script

    Args:
        user(str): snowflake username
        password(str): snowflake password
    """
    sfa = SnowFlakeAccess(user, password)

    # update tables and views
    sfa.backupSchema()
    sfa.updateSchema()

    # update yaml config dependencies
    fileDir = '/Users/mike.herrera/notebooks/snowflake/cached_history/jobs'
    yamlFile = os.path.join(fileDir, 'yaml.csv')

    td = datetime.datetime.today()

    # backup yaml config dependencies
    newYamlFile = os.path.join(fileDir, 'yaml_%s.csv' % td.strftime('%Y%m%d%H%M'))
    cmd = 'cp %s %s' % (yamlFile, newYamlFile)
    shutil.copyfile(yamlFile, newYamlFile)
    print("copied %s to %s" % (yamlFile, newYamlFile))

    workSpace='/Users/mike.herrera/workspace/'
    df = getYamlDependencies(workspace)
    df.to_csv(yamlFile, sep='|')
    print('written yaml dependency to %s' % yamlFile)


if __name__ == '__main__':
    user = '#####'
    password = '####'
    main(user, password)
