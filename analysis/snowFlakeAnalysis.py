"""this is snowflake analysis"""


import logging
import os
from glob import glob
import pandas as pd
import numpy as np
from google.cloud import storage
import networkx as nx
from mikesnowflake.access.snowFlakeAccess import SnowFlakeAccess
from mikesnowflake.access.bqAccess import BqAccess
from mikesnowflake.access.colorAccess import ColorAccess
from mikesnowflake.util.yamlUtil import getYamlConfig


GIT_DIR = '/Users/mike.herrera/workspace/data-sustain-snowflake-etl'


class SnowFlakeAnalysis(object):
    """this is mike's snowflake analysis class.
    """
    def __init__(self, startDate, endDate, user, password, gitDir=GIT_DIR, verbose=True, excludeEtl=True):
        """
        Args:
            startDate(datetime.datetime): the start of the analysis period
            endDate(datetime.datetime): the end of the analysis period
            user(str): snowflake username
            password(str): snowflake password
            gitDir(str, optional): the file directory path location of the git repo for https://github.com/openx/data-sustain-snowflake-etl.
            verbose(bool, optional): prints verbose statements
            excludeEtl(bool, optional): removes SNOWFLAKE_PROD_ETL from queries to reduce table hit noise

        Notes:
            I'm sure that there's a python library to parse github repos. However, I didn't feel like creating it. So instead, I locally
            downloaded them and used them as references in file paths.
        """
        logging.info('initializing snowflake analysis')
        self.startDate = startDate
        self.endDate = endDate
        self.excludeEtl = excludeEtl
        self.gitDir = gitDir

        # these are the main query types that would register any real usage to a system
        self.queryTypes = {'insert': ['INSERT', 'UPDATE', 'UNKNOWN',  'DELETE', 'COPY', 'MERGE', 'SET',
                                      'UNLOAD', 'BEGIN_TRANSACTION', 'TRUNCATE_TABLE','PUT_FILES', 'REMOVE_FILES'],
                           'select': ['SELECT', 'CREATE_TABLE_AS_SELECT'],
                           'admin': ['CREATE', 'CREATE_TABLE', 'ALTER', 'GRANT', 'REVOKE', 'DROP',
                                     'CREATE_TASK', 'ALTER_TABLE_MODIFY_COLUMN', 'ALTER_TABLE_ADD_COLUMN',
                                     'RENAME_TABLE', 'ALTER_TABLE', 'ALTER_MATERIALIZED_VIEW_LIFECYCLE',
                                     'USE', 'RECLUSTER', 'ALTER_TABLE_DROP_CLUSTERING_KEY',
                                     'ALTER_SESSION', 'RESTORE',
                                     'CREATE_CONSTRAINT', 'GET_FILES', 'LIST_FILES'],
                           'describe': ['DESCRIBE_QUERY', 'DESCRIBE', 'SHOW',]}
        ca = ColorAccess()
        cols = [col for columns in self.queryTypes.values() for col in columns]
        self.queryTypeColors = dict(zip(cols, ca.getColors(len(cols))))

        # this is extra info to allow us to cross reference SnowFlake tables and views.
        self.sfa = SnowFlakeAccess(user, password, verbose=verbose)
        self.snowFlakeTables = self.sfa.getTables()
        self.snowFlakeViewDefs = self.sfa.getViews()
        self.snowFlakeViews = self.snowFlakeViewDefs['name'].tolist()
        logging.info('obtained snowflake tables and views')

        self.bqa = BqAccess()

        if excludeEtl:
            logging.info("excluding SNOWFLAKE_PROD_ETL user from select statements.")

        # there are snowflake tables that are being unloaded into GCS. We make a note of them here.
        self.gcsTables = self.__getGcsTables()
        logging.info('obtained gcs table and view names')

        # we reorder the query types that have the biggest impact
        logging.info('obtaining hit breakdown')
        self.hitBreakdown = self.__getHitBreakdown()

        logging.info('getting yaml info')
        self.yamlInfo = self.__getYamlInfo()

        logging.info('created view directed graph of dependent table names')
        self.viewGraph = self.__getViewDepGraph()

        # create directed graph of views and dependent tables
        logging.info('created rollup directed graph of dependent table names')
        self.rollupGraph = self.__getRollupGraph()

        # create total table dependency graph
        logging.info('created total table dependency graph')
        self.tableGraph = nx.compose(self.viewGraph, self.rollupGraph)
        logging.info('calculating tablename dependency degrees')
        self.tableDegrees = pd.Series(dict(self.tableGraph.degree())).reindex(self.snowFlakeTables).fillna(0)

        logging.info('init complete')

    def __getRollupGraph(self):
        """this will return a graph of table names associated with rullup processes

        Returns:
            networkx.DiGraph: a directed graph of table names and associated rollups
        """
        R = nx.DiGraph()

        # parse odfi rollups
        odfiFiles = sorted(glob(os.path.join(self.gitDir, 'jobs', 'odfi_etls', '*.yaml')))
        for f in odfiFiles:
            data = getYamlConfig(f)
            if not 'ROLLUP_CONFIG' in data:
                continue

            key = list(data['ROLLUP_CONFIG'].keys())[0]
            if isinstance(data['ROLLUP_CONFIG'][key], list):
                continue

            if 'time_rollups' in data['ROLLUP_CONFIG'][key]:
                for k, v in data['ROLLUP_CONFIG'][key]['time_rollups'].items():
                    source = v['source'].upper()
                    target = v['table'].upper()
                    if source not in R.nodes():
                        R.add_node(source)
                    if target not in R.nodes():
                        R.add_node(target)
                    R.add_edge(source, target)

        # parse daily rollups
        dailyFile = os.path.join(self.gitDir, 'jobs', 'daily_rollups', 'daily_rollups.yaml')
        data = getYamlConfig(dailyFile)
        for key in ['ROLL_SQLS', 'ROLL_ADVT_SQLS']:
            for elem in data[key]:
                target = elem['label'].upper()
                source = elem['sql'].split('FROM ')[1].strip().split(' ')[0].upper()
                if source not in R.nodes():
                    R.add_node(source)
                if target not in R.nodes():
                    R.add_node(target)
                R.add_edge(source, target)

        # parse monthly rollups
        monthlyFile = os.path.join(self.gitDir, 'jobs', 'monthly_rollups', 'monthly_rollups.yaml')
        data = getYamlConfig(monthlyFile)
        for key in ['ROLL_SQLS', 'ROLL_ADVT_SQLS']:
            for elem in data[key]:
                if 'delete' in elem['label']:
                    continue
                target = elem['label'].upper()
                source = elem['sql'].split('FROM ')[1].strip().split(' ')[0].upper()
                if source not in R.nodes():
                    R.add_node(source)
                if target not in R.nodes():
                    R.add_node(target)
                R.add_edge(source, target)

        return R

    def __getGcsTables(self):
        """this will obtain a list of tables that are unloaded into GCS buckets

        Returns:
            list of str: a list of GCS table names
        """
        client = storage.Client(project='ox-data-prod')
        bucket = client.get_bucket('ox-data-prod-us-central1-reports')
        iterator = bucket.list_blobs(prefix='snowflake/', delimiter="/")
        response = iterator._get_next_page_response()

        return [os.path.basename(p[:-1]).upper() for p in response['prefixes']]

    def __getHitBreakdown(self):
        """Get top tables hits for all activity (excluding ETL user) and review tables with specific select hits.

        Returns:
            pd.DataFrame: a data frame of tables and corresponding hits. See Notes.

        Notes:
            The resulting data frame will have the following columns:
            
            'table_name' - the name of the table in snowflake
            'insert' - the collected count of insert-like statements made
            'select' - the collected count of select-like statements made
            'admin' - the collected count of db administrative statements made
            'describe' - the collected count of describe-like statements made
        """
        sql = ("SELECT th.table_name, th.query_type, SUM(th.hits) AS hits "
               "FROM (SELECT distinct table_name, query_type, query_id, 1 AS hits " +
               "FROM snowflake_test.table_history " +
               "WHERE query_date between '%s' AND '%s' " % (self.startDate.date(), self.endDate.date()))
        if self.excludeEtl:
            sql += ("AND (query_type not in (%s) " % str(self.queryTypes['select']).strip('[]') +
                    "OR user_name != 'SNOWFLAKE_PROD_ETL') ")
        sql += (") th " +
                "GROUP BY th.table_name, th.query_type")
        df = self.bqa.rawQuery(sql).pivot(index='table_name', columns='query_type', values='hits')
        df.columns.name = None

        # we need to confirm that all columns represented in the data frame are accounted for
        allCols = [col for columns in self.queryTypes.values() for col in columns]
        missingCols = [c for c in df.columns if c not in allCols]
        if len(missingCols) > 0:
            raise ValueError('the following columns were not classified. Please update your view definitions as well\n %s' % missingCols)

        # we create tallies for the various queryType categories
        for category, columns in self.queryTypes.items():
            df[category] = df[[c for c in df.columns if c in columns]].sum(axis=1)

        return df[self.queryTypes.keys()]

    def __getViewDepGraph(self):
        """this will return a graph of table names associated with view definitions

        Returns:
            networkx.DiGraph: a directed graph of table names and associated views
        """
        embeddedTableNames = {}  # this will be a dictionary of table names that are substrings of other table names
        for t1 in self.snowFlakeTables:
            for t2 in self.snowFlakeTables:
                if t1 in t2 and t1 != t2:
                    if t1 not in embeddedTableNames:
                        embeddedTableNames[t1] = []
                    embeddedTableNames[t1].append(t2)

        G = nx.DiGraph()
        for v in self.snowFlakeViews:
            G.add_node(v)
            viewDef = self.snowFlakeViewDefs.set_index('name').loc[v, 'text']
            for t in self.snowFlakeTables:
                if t.lower() not in viewDef.lower() and t.lower() + ' ' not in viewDef.lower():
                    continue

                status = True
                if t in embeddedTableNames:
                    otherFound = [name for name in embeddedTableNames[t] if name.lower() in viewDef.lower() and name.lower() != v.lower()]
                    if len(otherFound) > 0:
                        status = False
                if status:
                    if t != v:
                        if t not in G.nodes():
                            G.add_node(t)
                        G.add_edge(t, v)
        return G

    def __getYamlInfo(self):
        """
        """
        yamlFile = os.path.join(self.sfa.cacheDir, 'jobs', 'yaml.csv')
        yaml = pd.read_csv(yamlFile, sep='|', index_col=0)

        return yaml

    def getQueryTypeHistory(self, tableName):
        """
        """
        sql = ("SELECT query_date, query_type, count(query_id) as hits " +
               "FROM snowflake_test.table_history " +
               "WHERE query_date between '%s' and '%s' " % (self.startDate.date(), self.endDate.date()) +
               "AND table_name = '%s' " % tableName)
        if self.excludeEtl:
            sql += ("AND (query_type not in (%s) " % str(self.queryTypes['select']).strip('[]') +
                    "OR user_name != 'SNOWFLAKE_PROD_ETL') ")
        sql += "GROUP BY query_date, query_type "

        df = self.bqa.rawQuery(sql)
        return df

    def getUsageHistory(self, tableName, queryTypeGroup):
        """this will get you a user history for a given table and query type sql commands.

        Args:
            tableName(str): the name of the table for which you want to observe user history
            queryTypeGroup(str): 'insert', 'select', 'admin', 'describe'. See notes.

        Returns
            DataFrame: a stacked data frame of results. See notes.

        Notes:
            The resulting data frame will contain the following fields (based on what you use for queryTypeGroup)::

                query_type: the query types associated with query group (see the dictionary self.queryTypes)
                query_date: the date of the query_type
                user_name: the user who made the query
                hits: the count of distinct hits by query_type, query_date and user_name
        """
        sql = ("SELECT query_type, query_date, user_name, hits " +
               "FROM snowflake_test.v_%s_usage " % queryTypeGroup +
               "WHERE query_date between '%s' and '%s' " % (self.startDate.date(), self.endDate.date()) +
               "AND table_name = '%s' " % tableName +
               "ORDER BY query_date")

        df = self.bqa.rawQuery(sql)
        return df

    def printDropCommands(self, tableList):
        """
        """
        for tableName in tableList:
            cmd = 'drop table %s;' % tableName
            if tableName in self.snowFlakeViews:
                cmd = 'drop view %s;' % tableName
            print(cmd)

    @classmethod
    def printRetentionCommands(cls, tableList, days=21):
        """
        """
        for tableName in tableList:
            cmd = 'alter table %s set data_retention_time_in_days=%s;' % (tableName, days)
            print(cmd)

    def getQueryTextHistory(self, tableName):
        """
        """
        sql = ("SELECT t.query_date, t.user_name, t.query_id, t.query_type, " +
               "t.table_name, q.query_text " +
               "FROM snowflake_test.table_history as t " +
               "JOIN snowflake_test.query_history q " +
               "ON q.query_id = t.query_id " +
               "WHERE t.table_name = '%s' " % tableName)
        if self.excludeEtl:
            sql += ("AND (t.query_type not in (%s) " % str(self.queryTypes['select']).strip('[]') +
                    "OR t.user_name != 'SNOWFLAKE_PROD_ETL') ")

        sql += "ORDER BY t.query_date"
        df = self.bqa.rawQuery(sql)

        return df

    def getViewDefinition(self, tableName):
        """
        """
        viewDefs = self.snowFlakeViewDefs.set_index('name')
        if tableName in viewDefs.index:
            return viewDefs.loc[tableName, 'text']
        return None