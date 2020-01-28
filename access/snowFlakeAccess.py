import datetime
from dateutil.parser import parse
import os
import shutil
import pandas as pd
import snowflake.connector


# CACHE_DIR is where we store cached schema tables and views which sit by default right outside of the main project.
FILE_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.abspath(os.path.join(FILE_DIR, '..', 'cached_history'))


class SnowFlakeAccess(object):
    """snowflake connection class that uses pandas
    """
    def __init__(self, user, password, role='ACCOUNTADMIN', schema='mstr_datamart', database='PROD', warehouse='PROD_OTHER_WH',
                 cacheDir=CACHE_DIR, verbose=True):
        """init

        Args:
            user(str): the snowflake username
            password(str): the corresponding snowflake password
            role(str, optional): the user's role (defaults to 'ACCOUNTADMIN')
            schema(str, optional): snowflake schema (defaults to 'mstr_datamart')
            database(str, optional): snowflake database (defaults to 'PROD')
            warehouse(str, optional): snowflake warehouse (defaults to 'PROD_OTHER_WH')
            cacheDir(str, optional): the path to cached schema tables and views (defaults to subdirectory in this project)
            verbose(bool, optional): enables verbose printing when True
        """
        self.kwargs = {'account': 'openx',
                       'region': 'us-east-1',
                       'schema': schema,
                       'autocommit': False,
                       'paramstyle': 'qmark',
                       'timezone': 'UTC',
                       'database': database,
                       'warehouse': warehouse,
                       'user': user,
                       'password': password,
                       'role': role}
        self.cacheDir = cacheDir
        self.verbose = verbose

    def rawQuery(self, sql):
        """this method allows users to execute raw queries.

        Args:
            sql(str): the sql statement you want to execute.

        Returns:
            DataFrame: a pandas.DataFrame of the results
        """
        try:
            connection = snowflake.connector.connect(**self.kwargs)
            df = pd.io.sql.read_sql_query(sql, connection)
        finally:
            connection.close()
        return df

    def getViews(self):
        """this will read a cached file of view definitions currently in prod.

        Returns:
            DataFrame: a pandas.DataFrame of the results
        """
        fileName = os.path.join(self.cacheDir, 'schema', 'views.csv')
        if self.verbose:
            print('reading files from %s' % fileName)
        df = pd.read_csv(fileName, sep='|', index_col=0)

        return df

    def getTables(self):
        """this will read in a cached file of table names currently in prod.

        Returns:
            list of str: a list of table names
        """
        fileName = os.path.join(self.cacheDir, 'schema', 'tables.csv')
        if self.verbose:
            print('reading files from %s' % fileName)
        df = pd.read_csv(fileName, sep='|', index_col=0)

        return sorted(df['TABLE_NAME'].tolist())

    def backupSchema(self):
        """this will make copies of the current view and table cached files with a timestamp.
        """
        if self.verbose:
            print('backing up files')
        td = datetime.datetime.today()
        schemaDir = os.path.join(self.cacheDir, 'schema')

        # backup tables
        tableFile = os.path.join(schemaDir, 'tables.csv')
        newTableFile = os.path.join(schemaDir, 'tables_%s.csv' % td.strftime('%Y%m%d%H%M'))
        cmd = 'cp %s %s' % (tableFile, newTableFile)
        shutil.copyfile(tableFile, newTableFile)
        if self.verbose:
            print("copied %s to %s" % (tableFile, newTableFile))

        # backup views
        viewFile = os.path.join(schemaDir, 'views.csv')
        newViewFile = os.path.join(schemaDir, 'views_%s.csv' % td.strftime('%Y%m%d%H%M'))
        shutil.copyfile(viewFile, newViewFile)
        if self.verbose:
            print("copied %s to %s" % (viewFile, newViewFile))

    def updateSchema(self):
        """this will ping snowflake db for views and tables, saving them to a location on disk.
        """
        schemaDir = os.path.join(self.cacheDir, 'schema')

        # refresh view file, including business intelligence view info
        sql = "show views in prod.businessintelligence"
        if self.verbose:
            print(sql)
        biViews = self.rawQuery(sql)

        sql = "show views in prod.mstr_datamart"
        if self.verbose:
            print(sql)
        views = self.rawQuery(sql)
        views = views.append(biViews, ignore_index=True)

        viewFile = os.path.join(schemaDir, 'views.csv')
        views.to_csv(viewFile, sep='|')
        if self.verbose:
            print('live view schema saved to %s' % viewFile)

        # refresh table file, incuding views from business intelligence
        sql = ("SELECT DISTINCT table_name " +
               "FROM prod.information_schema.columns " +
               "WHERE table_schema = 'MSTR_DATAMART' " +
               "AND TABLE_NAME not in ('TEST', 'TS') " +
               "ORDER BY table_name")
        if self.verbose:
            print(sql)
        tables = self.rawQuery(sql)
        tables = pd.DataFrame(pd.concat([tables['TABLE_NAME'], biViews['name']])).rename(columns={0: 'TABLE_NAME'}).reset_index(drop=True)

        tableFile = os.path.join(schemaDir, 'tables.csv')
        tables.to_csv(tableFile, sep='|')
        if self.verbose:
            print('live table schema saved to %s' % tableFile)