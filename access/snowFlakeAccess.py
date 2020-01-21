import datetime
from dateutil.parser import parse
import os
import shutil
import pandas as pd
import snowflake.connector


class SnowFlakeAccess(object):
    """the new snowflake connection class that uses pandas
    """
    def __init__(self):
        """
        """
        self.kwargs = {'account': 'openx',
                       'region': 'us-east-1',
                       'schema': 'mstr_datamart',
                       'autocommit': False,
                       'paramstyle': 'qmark',
                       'timezone': 'UTC',
                       'database': 'PROD',
                       'warehouse': 'PROD_ETL_WH',
                       'user': '',  # fill in with your db username
                       'password': '',  # fill in with your db password
                       'role': ''}  # fill in with your db role

        self.cacheDir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cached_history')

    def rawQuery(self, sql):
        """
        """
        try:
            connection = snowflake.connector.connect(**self.kwargs)
            df = pd.io.sql.read_sql_query(sql, connection)
        finally:
            connection.close()
        return df

    def getViews(self):
        """
        """
        fileName = os.path.join(self.cacheDir, 'schema', 'views.csv')
        df = pd.read_csv(fileName, sep='|', index_col=0)

        return df

    def getTables(self):
        """
        """
        fileName = os.path.join(self.cacheDir, 'schema', 'tables.csv')
        df = pd.read_csv(fileName, sep='|', index_col=0)

        return sorted(df['TABLE_NAME'].tolist())

    def backupSchema(self):
        """
        """
        print('backing up files')
        td = datetime.datetime.today()
        schemaDir = os.path.join(self.cacheDir, 'schema')

        # backup tables
        tableFile = os.path.join(schemaDir, 'tables.csv')
        newTableFile = os.path.join(schemaDir, 'tables_%s.csv' % td.strftime('%Y%m%d%H%M'))
        cmd = 'cp %s %s' % (tableFile, newTableFile)
        shutil.copyfile(tableFile, newTableFile)
        print("copied %s to %s" % (tableFile, newTableFile))

        # backup views
        viewFile = os.path.join(schemaDir, 'views.csv')
        newViewFile = os.path.join(schemaDir, 'views_%s.csv' % td.strftime('%Y%m%d%H%M'))
        shutil.copyfile(viewFile, newViewFile)
        print("copied %s to %s" % (viewFile, newViewFile))

    def updateSchema(self):
        """
        """
        schemaDir = os.path.join(self.cacheDir, 'schema')

        # refresh view file, including business intelligence view info
        sql = "show views in prod.businessintelligence"
        print(sql)
        biViews = self.rawQuery(sql)

        sql = "show views in prod.mstr_datamart"
        print(sql)
        views = self.rawQuery(sql)
        views = views.append(biViews, ignore_index=True)

        viewFile = os.path.join(schemaDir, 'views.csv')
        views.to_csv(viewFile, sep='|')
        print('live view schema saved to %s' % viewFile)

        # refresh table file, incuding views from business intelligence
        sql = ("SELECT DISTINCT table_name " +
               "FROM prod.information_schema.columns " +
               "WHERE table_schema = 'MSTR_DATAMART' " +
               "AND TABLE_NAME not in ('TEST', 'TS') " +
               "ORDER BY table_name")
        print(sql)
        tables = self.rawQuery(sql)
        tables = pd.DataFrame(pd.concat([tables['TABLE_NAME'], biViews['name']])).rename(columns={0: 'TABLE_NAME'}).reset_index(drop=True)

        tableFile = os.path.join(schemaDir, 'tables.csv')
        tables.to_csv(tableFile, sep='|')
        print('live table schema saved to %s' % tableFile)
