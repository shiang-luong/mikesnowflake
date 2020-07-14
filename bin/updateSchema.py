"""this would backup schema tables and views"""
import argparse
import os
import sys
import logging

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


def run(args):
    """
    """
    sfa = SnowFlakeAccess(args.user, args.password)

    # update tables and views
    sfa.backupSchema()
    sfa.updateSchema()

    # update yaml config dependencies
    yamlDir = os.path.join(sfa.cacheDir, 'jobs')
    yamlFile = os.path.join(yamlDir, 'yaml.csv')

    td = datetime.datetime.today()

    # backup yaml config dependencies
    newYamlFile = os.path.join(yamlDir, 'yaml_%s.csv' % td.strftime('%Y%m%d%H%M'))
    cmd = 'cp %s %s' % (yamlFile, newYamlFile)
    shutil.copyfile(yamlFile, newYamlFile)
    logging.info("copied %s to %s" % (yamlFile, newYamlFile))

    df = getYamlDependencies(PROJ_DIR)
    df.to_csv(yamlFile, sep='|')
    logging.info('written yaml dependency to %s' % yamlFile)

def main():
    """
    """
    parser = argparse.ArgumentParser(description='SnowFlake update schema')
    parser.add_argument("--user", default=None, help="SnowFlake user")
    parser.add_argument("--password", default=None, help="SnowFlake password")
    args = parser.parse_args()

    logging.getLogger().setLevel(logging.INFO)
    run(args)


if __name__ == '__main__':
    main()
