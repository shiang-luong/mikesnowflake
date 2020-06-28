import os
import sys
import logging

PROJ_DIR = os.path.dirname(os.path.abspath(os.path.join(__file__, '..', '..')))
sys.path.append(PROJ_DIR)

import argparse
import datetime
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
from mikesnowflake.access.snowFlakeAccess import SnowFlakeAccess
from mikesnowflake.access.bqAccess import BqAccess


os.nice(20)


PROJECT_ID = 'ox-data-devint'
BUCKET_ID = 'snowflake2bigquery'
DATASET_ID = 'snowflake_test'

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/mike.herrera/.config/google/ox-data-devint-8fddac53cd8a.json'


class Loader(object):
    """this is a loader from snowflake usage tables into bigquery"""

    def __init__(self, user, password, projectId=PROJECT_ID, bucketId=BUCKET_ID, datasetId=DATASET_ID):
        """init"""
        self.__bqa = BqAccess()
        self.__sfa = SnowFlakeAccess(user, password)
        self.__snowFlakeTables = self.__sfa.getTables()
        self.__embeddedTableNames = self.__getEmbeddedTableNames()

        self.__cacheDir = self.__sfa.cacheDir

        self.__projectId = projectId
        self.__bucketId = bucketId
        self.__datasetId = datasetId

        self.__bqClient = bigquery.Client(project=self.__projectId)
        self.__queryHistoryTable = self.__bqClient.get_dataset(self.__datasetId).table('query_history')
        self.__tableHistoryTable = self.__bqClient.get_dataset(self.__datasetId).table('table_history')

        self.__gcsClient = storage.Client(project=self.__projectId)
        self.__gcsBucket = self.__gcsClient.get_bucket(self.__bucketId)

        # this is the bq job config for the query history table
        jobCfg = bigquery.LoadJobConfig()
        jobCfg.skip_leading_rows = 1
        jobCfg.source_format = bigquery.SourceFormat.CSV
        jobCfg.allow_quoted_newlines = True
        jobCfg.autodetect
        jobCfg.field_delimiter = '|'
        jobCfg.max_bad_records = 0
        jobCfg.write_disposition = 'WRITE_APPEND'
        self.__queryHistCfg = jobCfg

        # this is the bq job config for the query history table
        jobCfg = bigquery.LoadJobConfig()
        jobCfg.skip_leading_rows = 1
        jobCfg.source_format = bigquery.SourceFormat.CSV
        jobCfg.autodetect
        jobCfg.field_delimiter = '|'
        jobCfg.max_bad_records = 0
        jobCfg.write_disposition = 'WRITE_APPEND'
        self.__tableHistCfg = jobCfg

    def __getEmbeddedTableNames(self):
        """
        """
        embeddedTableNames = {}  # this will be a dictionary of table names that contain themselves in other table names
        for t1 in self.__snowFlakeTables:
            for t2 in self.__snowFlakeTables:
                if t1 in t2 and t1 != t2:
                    if t1 not in embeddedTableNames:
                        embeddedTableNames[t1] = []
                    embeddedTableNames[t1].append(t2)

        return embeddedTableNames

    def saveTableHistory(self, when, tableOverride=None, uploadToBq=False):
        """
        """
        logging.info('pinging bq query history for %s' % when.date())
        inClause = ' OR '.join(["STRPOS(UPPER(query_text), '%s') != 0" % tableName for tableName in self.__snowFlakeTables])
        sql = ("SELECT query_date, user_name, query_type, query_id, query_text " +
               "FROM snowflake_test.query_history " +
               "WHERE query_date = '%s' " % when.date() +
               "AND (%s) " % inClause)
        queryHistory = self.__bqa.rawQuery(sql)
        
        if tableOverride:
            tableNames = [tableOverride]
            logging.info('setting table names to %s' % tableNames)
        else:
            tableNames = self.__snowFlakeTables
            logging.info('setting table names to entire snowflake universe')

        logging.info('iterating through query history to obtain table refs')
        data = []
        groupCols = ['user_name', 'query_id', 'query_type', 'query_text', 'query_date']
        for (user, query_id, query_type, query, dt), _ in queryHistory.groupby(groupCols):
            for tableName in tableNames:
                # we generally check that the table name (or equiv string refs are in the query) to continue
                if tableName.lower() not in query.lower() and tableName.lower() + ' ' not in query.lower():
                    continue

                # we check that the table name is not contained in other table names to reduce double-counting.
                # this catches attribution for instances like "DIM_SITES" and "DIM_SITES_TO_OWNERS"
                status = True
                if tableName in self.__embeddedTableNames:
                    otherFound = [name for name in self.__embeddedTableNames[tableName] if name.lower() in query.lower()]
                    if len(otherFound) > 0:
                        status = False

                if status:
                    data.append([dt, user, query_id, query_type, tableName])
        df = pd.DataFrame(data, columns=['QUERY_DATE', 'USER_NAME', 'QUERY_ID', 'QUERY_TYPE', 'TABLE_NAME'])
        logging.info('finished collecting table refs')

        # cache to disk, load to gcs then into bq
        if uploadToBq:
            baseName = 'tableHits_%s.csv' % when.strftime('%Y%m%d')
            if tableOverride:
                baseName = 'tableHits_%s_%s.csv' % (tableOverride, when.strftime('%Y%m%d'))
            fileName = os.path.join(self.__cacheDir, baseName)

            df.to_csv(fileName, sep='|')
            logging.info('saved %s' % fileName)

            logging.info('uploading to gcs')
            blobName = os.path.join('mike_logs', 'table_history', baseName)
            uri = os.path.join('gs://', self.__bucketId, blobName)
            blob = self.__gcsBucket.blob(blobName)
            blob.upload_from_filename(fileName)
            logging.info('uploaded file to %s' % uri)

            # delete previous entries in query history table (noting that tableOverride is only one entry)
            if tableOverride:
                delSql = ("DELETE FROM snowflake_test.table_history " +
                          "WHERE query_date = '%s' " % when.date() +
                          "AND table_name = '%s' " % tableOverride)
            else:
                delSql = "DELETE FROM snowflake_test.table_history WHERE query_date = '%s' " % when.date()

            self.__bqa.rawQuery(delSql)
            logging.info(delSql)

            # load blob into bq using the query history job config
            load_job = self.__bqClient.load_table_from_uri(uri, self.__tableHistoryTable, job_config=self.__tableHistCfg)
            logging.info("Starting job %s " % load_job.job_id)

            load_job.result()  # Waits for table load to complete.
            logging.info("Job finished.")

    def __checkQueryJobs(self, jobIds, queryTimeout=60, location='US'):
        """this internal method will check the state of BQ job ids for the following::

             * timeouts
             * query errors for the associated jobs

        Args:
            jobIds(list of str): a list of bq job ids
            queryTimeout(int): a timeout threshold for the queries to complete.
            location(str, optional): the default is 'US' for prebid-data queries.

        Returns:
            None: this doesn't return anything.

        Raises:
            ValueError: a value error if there are timeouts or errors. See Notes.

        Notes:
            If a timeout occurs, then we raise a message about the job id, the job state and the timeout limit.
            If a query error occurs then we raise a message about the job id, the reason, the complete error message
            and the original query.
        """
        # we check the job states and ensure that nothing is left hanging.
        startTs = datetime.datetime.now()
        status = False
        timeout = 0
        while not (status or timeout >= queryTimeout):
            states = [self.__bqClient.get_job(jobId, location=location).state for jobId in jobIds]
            if set(states) == {'DONE'}:
                status = True
            endTs = datetime.datetime.now()
            timeout = (endTs - startTs).seconds

        # if status is still False then we have hung somewhere after the timeout.
        # we report the last known states and raise a timeout exeption.
        if not status:
            msg = ""
            for jobId in jobIds:
                state = self.__bqClient.get_job(jobId, location=location).state
                if state != 'DONE':
                    msg += msg + "jobId={} is currently in state {} after {} seconds".format(jobId, state, queryTimeout)

            logging.error(msg)
            raise ValueError("timeout: {}".format(msg))

        # we also check for any errors and raise an exception
        msg = ''
        for jobId in jobIds:
            job = self.__bqClient.get_job(jobId, location=location)
            error = job.error_result
            if error:
                status = False
                reason = error['reason']
                errMsg = error['message']
                query = job.query
                msg += (msg +
                        'jobId=%s returned a query error.\n' % jobId +
                        'reason=%s;\n' % reason +
                        'message=%s;\n' % errMsg +
                        'query="%s";\n\n' % query)
                logging.error(msg)
        if msg != '':
            raise ValueError("query error: {}".format(msg))

    def saveQueryHistory(self, startDate, endDate):
        """
        """
        # save query history to GCS
        uris = []
        for when in pd.date_range(startDate, endDate):
            logging.info("pinging snowflake query history for %s" % when.date())
            startTime = when.replace(hour=0, minute=0, second=0, microsecond=0)
            endTime = when.replace(hour=23, minute=59, second=59)
            sql = ("SELECT DISTINCT DATABASE_NAME, SCHEMA_NAME, USER_NAME, ROLE_NAME, WAREHOUSE_NAME, " +
                   "START_TIME, QUERY_ID, QUERY_TYPE, QUERY_TEXT " +
                   "FROM snowflake.account_usage.query_history " +
                   "WHERE DATABASE_NAME = 'PROD' " +
                   "AND EXECUTION_STATUS = 'SUCCESS' " +
                   "AND start_time BETWEEN '%s' AND '%s'" % (startTime, endTime))
            df = self.__sfa.rawQuery(sql)
            df['QUERY_TEXT'] = df['QUERY_TEXT'].apply(lambda x: x.replace('\r', ' '))
            df['QUERY_DATE'] = pd.to_datetime(df['START_TIME'].apply(lambda x: x.date()))

            # save file to local disk then upload to gcs bucket blob
            baseName = 'queryHistory_%s.csv' % when.strftime('%Y%m%d')
            fileName = os.path.join(self.__cacheDir, baseName)
            df.to_csv(fileName, sep='|')
            logging.info('saved to file: %s' % fileName)

            logging.info('uploading to gcs')
            blobName = os.path.join('mike_logs', 'query_history', baseName)
            uri = os.path.join('gs://', self.__bucketId, blobName)
            blob = self.__gcsBucket.blob(blobName)
            blob.upload_from_filename(fileName)
            logging.info('uploaded file to %s' % uri)
            uris.append(uri)

        # delete previous entries in query history table
        jobIds = []
        for when in pd.date_range(startDate, endDate):
            delSql = "DELETE FROM snowflake_test.query_history WHERE query_date = '%s' " % when.date()
            delJob = self.__bqClient.query(delSql)
            logging.info(delSql)
            jobIds.append(delJob.job_id)
        self.__checkQueryJobs(jobIds)
        logging.info('done deleting dates!')

        # load blobs from GCS into bq
        load_job = self.__bqClient.load_table_from_uri(uris, self.__queryHistoryTable, job_config=self.__queryHistCfg)
        logging.info("Starting job %s " % load_job.job_id)
        load_job.result()  # Waits for table load to complete.
        logging.info("Job finished.")


def runLoad(args):
    """
    """
    endDate = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    if args.endDate:
        if args.endDate == 'yesterday':
            endDate = endDate - datetime.timedelta(days=1)
        else:
            endDate = datetime.datetime.strptime(args.endDate, '%Y%m%d')

    startDate = endDate
    if args.startDate:
        startDate = datetime.datetime.strptime(args.startDate, '%Y%m%d')

    loader = Loader(args.user, args.password)
    if args.tableOverride:
        for when in pd.date_range(startDate, endDate):
            loader.saveTableHistory(when, tableOverride=args.tableOverride, uploadToBq=True)
    else:
        loader.saveQueryHistory(startDate, endDate)
        for when in pd.date_range(startDate, endDate):
            loader.saveTableHistory(when, uploadToBq=True)


def main():  # pragma: no cover
    """
    """
    parser = argparse.ArgumentParser(description='SnowFlake query history to bq')
    parser.add_argument("--user", default=None, help="SnowFlake user")
    parser.add_argument("--password", default=None, help="SnowFlake password")
    parser.add_argument("--tableOverride", default=None, help="single table name to load into bq")
    parser.add_argument("--startDate", default=None, help="start date")
    parser.add_argument("--endDate", default=None, help="end date")
    args = parser.parse_args()

    logging.getLogger().setLevel(logging.INFO)
    runLoad(args)


if __name__ == '__main__':  # pragma: no cover
    main()