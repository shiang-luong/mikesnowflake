"""this would backup schema tables and views"""

import os
import datetime
import yaml
import subprocess
import shutil
import pandas as pd
from snowFlakeAccess import SnowFlakeAccess

os.nice(20)


def getNestedVals(searchObj):
    """
    Takes an object with nested lists and dicts and flattens all values into a single list.

    Notes:
        we drop any non-string values, since they wouldn't represent a sql statement
    """
    vals = []
    if isinstance(searchObj, str):
        vals.append(searchObj)
    elif isinstance(searchObj, list):
        for item in searchObj:
            vals.extend(getNestedVals(item))
    elif isinstance(searchObj, dict):
        for key, value in searchObj.items():
            vals.extend(getNestedVals(value))
            vals.extend(getNestedVals(key))

    return vals


def getYamlConfig(fileName):
    """
    """
    data = None
    with open(fileName) as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
    return data

def getYamlDependencies(workSpace='/Users/mike.herrera/workspace/', repo='data-sustain-snowflake-etl',
                        snowFlakeTables=None):
    """
    """
    sfa = SnowFlakeAccess()

    if not snowFlakeTables:
        snowFlakeTables = sfa.getTables()

    embeddedTableNames = {}  # this will be a dictionary of table names that contain themselves in other table names
    for t1 in snowFlakeTables:
        for t2 in snowFlakeTables:
            if t1 in t2 and t1 != t2:
                if t1 not in embeddedTableNames:
                    embeddedTableNames[t1] = []
                embeddedTableNames[t1].append(t2)    

    gitDir = os.path.join(workSpace, repo)
    res = []
    for tableName in snowFlakeTables:
        # special handling for DOWNLOAD_STATE table, since it's not yet in git but is a prod
        # process according to sigmoid (i.e. nuthan)
        if tableName == 'DOWNLOAD_STATE':
            yamlFiles = ['SnowflakeToGCS/BidPerformanceReport/scripts.yaml',
                         'SnowflakeToGCS/load_state.yaml']
            for yamlFile in yamlFiles:
                print('**SPECIAL CASE** %s found in %s' % (tableName, yamlFile))
                res.append([tableName, 'BidPerformance', 'gcs', None, 'SnowflakeToGCS', yamlFile])

            # these tables are also part of one of the configs
            otherTables = ['ADVERTISER_DIM', 'AD_UNIT_DIM', 'BRAND_DIM', 'DEAL_DIM', 'DEMAND_PARTNER_DIM',
                           'IAS_BUYER_BRAND_SUM_HOURLY_FACT_VIEW', 'OX_BUYER_BRAND_SUM_HOURLY_FACT',
                           'OX_COUNTRY_REGION_MAPPING', 'PACKAGE_DIM', 'PMP_DEAL_TYPE_MAPPING_DIM',
                           'PUBLISHER_DIM']
            for otherTable in otherTables:
                yamlFile = 'SnowflakeToGCS/BidPerformanceReport/scripts.yaml'
                print('**SPECIAL CASE** %s found in %s' % (otherTable, yamlFile))
                res.append([otherTable, 'BidPerformance', 'gcs', None, 'SnowflakeToGCS', yamlFile])

            continue
        # MONITOR_SF_LOAD is used by the Salesforce tables. However, we independently link it to the embdeded python script
        # that references it.
        if tableName == 'MONITOR_SF_LOAD':
            pyFile = 'data-sustain-snowflake-wheels/py-salesforce-pull/ox_dw_snowflake_salesforce_pull/settings.py'
            print('**SPECIAL CASE** %s found in %s' % (tableName, pyFile))
            res.append([tableName, None, None, None, 'data-sustain-snowflake-wheels', pyFile])            

        cmd = ['grep', tableName, '-l', '-i', '-R', '%s' % gitDir]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        out = p.communicate()[0].decode().split('\n')
        yamlFiles = sorted(list(set(f for f in out if f.endswith('.yaml'))))

        # salesforce tables reference the env yaml file. So we use the sample env file as a proxy.
        if tableName.startswith('SF_'):
            yamlFile = os.path.join(gitDir, 'conf', 'env.sample.yaml')
            yamlFiles.append(yamlFile)
            yamlFiles = sorted(set(yamlFiles))

        # content topic tables use a config that's not a part of the default repo. So we manually append. 
        if tableName.startswith('CONTENT_TOPIC'):
            yamlFile = os.path.join(workSpace, 'data-sustain-snowflake-wheels', 'py-odfi-etl',
                                    'ox_dw_snowflake_odfi_etl', 'app_config', 'content_topics.yaml')
            yamlFiles.append(yamlFile)

        # rollup_queue uses a config that's not a part of the default repo. So we manually append. 
        if tableName =='ROLLUP_QUEUE':
            yamlFile = os.path.join(workSpace, 'data-sustain-snowflake-wheels', 'py-odfi-etl',
                                    'ox_dw_snowflake_odfi_etl', 'app_config', 'rollup.yaml')
            yamlFiles.append(yamlFile)

        for yamlFile in yamlFiles:
            data = getYamlConfig(yamlFile)

            loadStateVar = None
            feedName = None
            feedLocation = None
            if isinstance(data, dict):
                loadStateVar = data.get('LOAD_STATE_VAR')
                feedName = data.get('FEED_NAME')
                feedLocation = data.get('FEED_LOCATION')
                skipKeys = ['LOAD_STATE_VAR', 'FEED_NAME', 'FEED_LOCATION']
                data = {k: v for (k, v) in data.items() if k not in skipKeys}

            # collect all sql statements in the yaml file in its various forms
            statements = getNestedVals(data)
            if len(statements) == 0:
                if sorted(data.keys()) != ['MAX_ATTEMPTS', 'WAIT_BETWEEN_ATTEMPTS']:
                    raise ValueError('no statements found')

            # append extra information about salesforce tables, which can't be auto-parsed
            if isinstance(data, dict) and 'SF_OBJECT_NAMES' in data:
                statements.extend(['SF_%s' % sfName.upper() for sfName in data['SF_OBJECT_NAMES']])
            # reduce statements that reference the table
            statements = [stmt for stmt in statements if (tableName.lower() in stmt.lower() or '%s ' % tableName.lower() in stmt.lower()) and '%s_tmp' % tableName.lower() not in stmt.lower()]
            if len(statements) > 0:
                count = 0 # a counter to see if the table is found after iterating through all statements
                for sql in statements:
                    # this catches attribution for instances like "DIM_SITES" and "DIM_SITES_TO_OWNERS".
                    # we exclude salesforce tables from this analysis, since they are uniquely configured in an env.yaml file
                    status = True
                    if tableName in embeddedTableNames and not tableName.startswith('SF_'):
                        otherFound = [name for name in embeddedTableNames[tableName] if name.lower() in sql.lower()]
                        if len(otherFound) > 0:
                            status = False
                    count += status
                if count > 0:
                    yamlFile = yamlFile.replace(workSpace, '')
                    yamlRepo = yamlFile.split('/')[0]
                    res.append([tableName, feedName, feedLocation, loadStateVar, yamlRepo, yamlFile])
                    print('%s found in %s' % (tableName, yamlFile))
    cols = ['table_name', 'feed_name', 'feed_location', 'load_state_var', 'repo', 'file']
    df = pd.DataFrame(res, columns=cols)

    return df


def main():
    """
    """
    sfa = SnowFlakeAccess()

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
    #shutil.copyfile(yamlFile, newYamlFile)
    print("copied %s to %s" % (yamlFile, newYamlFile))

    df = getYamlDependencies()
    df.to_csv(yamlFile, sep='|')
    print('written yaml dependency to %s' % yamlFile)


if __name__ == '__main__':
    main()
