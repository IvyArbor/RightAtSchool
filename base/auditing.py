import abc #Define and use abstract base classes for API checks in your code
from datetime import datetime
import traceback
import os.path
from abc import ABC, abstractmethod

class Auditable(ABC):
    def configureAudit(self):
        if self.verbose:
            print('Prepare audit for the job', self.__class__.__name__)

        self.audit_connection = self.getDatabaseConnection('AUDIT')

        self.audit = ETLAudit(
            job_name = self.__class__.__name__,
            user_name = self.user_name,
            source_database = self.source_database,
            source_table = self.source_table,
            target_database = self.target_database,
            target_table = self.target_table,
            connection = self.audit_connection,
            verbose = self.verbose,
            debug = self.debug,
        )

    def beforeJobAudit(self):
        # Insert Job Execution Record
        self.audit.insertAuditJobExecution()

        self.last_job_extract_time = self.audit.getLastExtractionTime()
        if self.verbose:
            print("Data extraction starts from:", self.last_job_extract_time)

        # Insert Job Extract Time
        self.audit.insertAuditExtractionTime(self.last_job_extract_time)

        # Connected later so we can catch errors if doesn't succeed
        self.target_connection = self.getDatabaseConnection(self.target_database)

        # Initial Row Count (target table)
        initial_row_count = self.audit.targetRowCount(
            self.target_connection,
            self.target_table,
        )

        # Insert Table Process Record
        self.audit.insertAuditTableProcessing(initial_row_count)

    def afterJobAudit(self, counts):
        # Initial Row Count (target table)
        counts['final'] = self.audit.targetRowCount(
            self.target_connection,
            self.target_table,
        )

        # Update Table Process Record
        self.audit.updateAuditTableProcessing(counts)
        # Update Extract Time Record
        self.audit.updateAuditExtractionTime()
        # Update Job Execution
        self.audit.updateAuditJobExecution()
        self.audit_connection.close()

    def logWarning(self, row, line):
        if self.verbose:
            print('Error while inserting row:', row)
        self.audit.logETLWarning(str(row), line)

    def logError(self, exc):
        ext_tb = traceback.extract_tb(exc.__traceback__)
        tb = ext_tb[-1]
        msg = "\n".join(traceback.format_list(ext_tb))
        # filename = os.path.relpath(tb.filename)
        filename = tb.filename

        print('Exception:', str(exc))
        print(msg)

        self.audit.logETLError(
            error_procedure = filename, # + "\n" + tb.line,
            error_line = tb.lineno,
            error_code = None,
            error_message = "Exception: " + str(exc) + "\n" + msg
        )
        self.audit.updateErrorJobExecution()
        self.audit.rollbackAuditExtractionTime()
        self.audit_connection.close()

    def updateAuditTracking(self, row):
        row['InsertDate'] = row['UpdateDate'] = datetime.utcnow()
        row['InsertAuditKey'] = row['UpdateAuditKey'] = self.audit.JobExecutionKey

    @abstractmethod
    def getDatabaseConnection(self, name):
        pass


class ETLAudit(object):
    def __init__(self, job_name, user_name,
                source_database, source_table, target_database, target_table, \
                connection, parent_job = 0, verbose = False, debug = False):
        self.start_date = datetime.utcnow()
        self.success_indicator = ['Y', 'N', None]
        self.job_name = job_name
        self.user_name = user_name
        self.source_database = source_database,
        self.source_table = source_table,
        self.target_database = target_database,
        self.target_table = target_table,
        self.connection = connection
        self.parent_job = parent_job
        self.verbose = verbose
        self.debug = debug


    # Get the extract time from the last execution
    def getLastExtractionTime(self):
        query = """
            SELECT `ExtractStopDate`
            FROM `auditextractiontime`
            WHERE `JobName` = %s
            ORDER BY `ExtractStopDate` DESC
            LIMIT 1
        """
        with self.connection.cursor() as cursor:
            self.verbose_print(query)
            self.debug_print('Looking up for the last extract time: auditextractiontime.')
            cursor.execute(query, (self.job_name))
            row = cursor.fetchone()
            return row[0] if row != None else '1000-01-01 00:00:00'

    # Insert Job Execution Record
    def insertAuditJobExecution(self):
        self.JobExecutionKey = self.insert('auditjobexecution', {
            'JobName': self.job_name,
            'ExecutionStartDate': self.start_date,
            # 'ExecutionStopDate': DEFAULT NULL,
            # 'SuccessfulProcessingIndicator': DEFAULT NULL,
            'ParentJobExecutionKey': self.parent_job,
            # 'ExtractionTimeKey': DEFAULT NULL, # SHould this be -1 ???
        })

    # Insert Job Extract Time
    def insertAuditExtractionTime(self, start_date):
        self.ExtractionTimeKey = self.insert('auditextractiontime', {
            'JobName': self.job_name,
            'ExtractStartDate': start_date,
            # 'ExtractStopDate': DEFAULT NULL,
            # 'Comments': DEFAULT NULL,
            'LastUpdated': self.start_date,
        })

    # Initial Row Count (target table)
    def targetRowCount(self, target_connection, table):
        query = "SELECT COUNT(*) FROM `{}`".format(table)
        with target_connection.cursor() as cursor:
            cursor.execute(query)
            row_count = cursor.fetchone()
        return row_count

    # Insert Table Process Record
    def insertAuditTableProcessing(self, initial_row_count):
        self.TableProcessKey = self.insert('audittableprocessing', {
            'JobExecutionKey': self.JobExecutionKey,
            'SourceDatabase': self.source_database,
            'SourceTableName': self.source_table,
            'TargetDatabase': self.target_database,
            'TargetTableName': self.target_table,
            'InitialRowCount': initial_row_count,
            # 'ExtractRowCount': DEFAULT NULL,
            # 'InsertRowCount': DEFAULT NULL,
            # 'UpdateRowCnt': DEFAULT NULL,
            # 'ErrorRowCount': DEFAULT NULL,
            # 'FinalRowCount': DEFAULT NULL,
            # 'SuccessfulProcessingIndicator': DEFAULT NULL,
        })

    def updateAuditTableProcessing(self, counts):
        self.update(
            'audittableprocessing',
            {
                'ExtractRowCount': counts['extract'],
                'InsertRowCount': counts['insert'],
                'UpdateRowCnt': counts['update'],
                'ErrorRowCount': counts['error'],
                'FinalRowCount': counts['final'],
                'SuccessfulProcessingIndicator': self.success_indicator[0],
            },
            {'TableProcessKey': self.TableProcessKey}
        )

    def updateAuditExtractionTime(self):
        self.update(
            'auditextractiontime',
            {
                'ExtractStopDate': datetime.utcnow(),
                'LastUpdated': datetime.utcnow(),
            },
            {'ExtractionTimeKey': self.ExtractionTimeKey}
        )

    def updateAuditJobExecution(self):
        self.update(
            'auditjobexecution',
            {
                'ExtractionTimeKey': self.ExtractionTimeKey,
                'ExecutionStopDate': datetime.utcnow(),
                'SuccessfulProcessingIndicator': self.success_indicator[0],
            },
            {'JobExecutionKey': self.JobExecutionKey}
        )

    def logETLWarning(self, row_contents, extracted_line):
        self.WarningLogKey = self.insert('auditwarninglog', {
            'TableProcessKey': self.TableProcessKey,
            'WarningDate': datetime.utcnow(),
            'WarningExtractLine': extracted_line,
            'WarningRowContents': row_contents,
        })

    def logETLError(self, error_procedure = None, error_line = None, \
                    error_code = None, error_message = None):
        self.ErrorLogKey = self.insert('auditerrorlog', {
            'JobExecutionKey': self.JobExecutionKey,
            'TableName': self.source_table,
            'UserName': self.user_name,
            'ErrorDate': datetime.utcnow(),
            'ErrorProcedure': error_procedure,
            'ErrorLine': error_line,
            'ErrorCode': error_code,
            'ErrorMessage': error_message,
        })

    def updateErrorJobExecution(self):
        self.update(
            'auditjobexecution',
            {
                'ExtractionTimeKey': self.ExtractionTimeKey,
                'ExecutionStopDate': datetime.utcnow(),
                'SuccessfulProcessingIndicator': self.success_indicator[1],
            },
            {'JobExecutionKey': self.JobExecutionKey}
        )

    def rollbackAuditExtractionTime(self):
        self.update(
            'auditextractiontime',
            {
                'ExtractStopDate': self.start_date,
                'Comments': 'ExtractStopDate was reset due to an error in the <{}>.'.format(self.job_name),
                'LastUpdated': datetime.utcnow(),
            },
            {'ExtractionTimeKey': self.ExtractionTimeKey}
        )

    def insert(self, table, dict):
        with self.connection.cursor() as cursor:
            columns = ', '.join(['`{}`'.format(key) for key in dict.keys()])
            placeholders = ', '.join(['%s'] * len(dict))
            query = "INSERT INTO `{}` ({}) VALUES ({})".format(table, columns, placeholders)
            self.verbose_print(query)
            self.debug_print('Before insert: {}.'.format(table))
            try:
                cursor.execute(query, tuple(dict.values()))
                self.connection.commit()
                inserted_id = cursor.lastrowid
                self.verbose_print("Inserted id: " + str(inserted_id))
                self.debug_print('After insert: {}.'.format(table))
                return inserted_id
            except Exception as e:
                if self.verbose:
                    print(e)
                return None

    def update(self, table, dict, where):
        with self.connection.cursor() as cursor:
            set_columns = ', '.join(['`{}` = %s'.format(key) for key in dict.keys()])
            where_columns = ' AND '.join(['`' + key + '` = %s' for key in where.keys()])
            query = "UPDATE `{}` SET {} WHERE {}".format(table, set_columns, where_columns)
            self.verbose_print(query)
            self.debug_print('Before update: {}.'.format(table))
            try:
                affected_rows = cursor.execute(query, tuple(dict.values()) + tuple(where.values()))
                self.connection.commit()
                self.verbose_print("Affected rows: " + str(affected_rows))
                self.debug_print('After update: {}.'.format(table))
                return affected_rows
            except Exception as e:
                if self.verbose:
                    print(e)
                return None

    def debug_print(self, msg):
        if self.debug:
            input("==> " + msg + " Press Enter to continue...")

    def verbose_print(self, msg):
        if self.verbose:
            print(msg)
