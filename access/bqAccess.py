"""this is a simple access layer to GBQ"""


import datetime
import pandas_gbq


class BqAccess(object):
    """this is a pandas-style access class for big query"""

    def __init__(self):
        """init

        Notes:
        This access layer presumes that you have a env variable defined as follows:
        GOOGLE_APPLICATION_CREDENTIALS="<path-to-your-json-auth-file"
        """

    def rawQuery(self, sql):
        """this will send the sql to BQ and return the results

        Args:
            sql(str): the sql string you care about

        Returns:
            DataFrame: a pandas.DataFrame of the results
        """
        df = pandas_gbq.read_gbq(sql)
        return df

    def deleteTableHistory(self, when, writeToDb=False):
        """this will delete query history from ox-data-devint.snowflake_test.table_history

        Args:
            startDate(datetime.datetime)
            endDate(datetime.datetime)
        """
        sql = ("DELETE FROM snowflake_test.table_history " +
               "WHERE query_date = '%s' " % when.date())
        if writeToDb:
            self.rawQuery(sql)
        return sql