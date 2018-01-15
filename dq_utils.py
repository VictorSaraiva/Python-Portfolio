"""
********************************************************************************
Description:
This module offers all the functionality to handle all the steps required to 
take XML assessment files and correction logs from DQ application to the DQ reporting DB. 

Ver   Date         Author                    Description
---   --------     -----------------------   ----------------------------------
0.1   22-FEB-2016  Victor Saraiva            Initial Version
********************************************************************************
"""

import os
import shutil
from datetime import datetime, timedelta
from itertools import chain
import collections
import csv
import sys
import operator
import smtplib
import base64, re
from os.path import basename
import mimetypes
from email.mime.multipart import MIMEMultipart
from email import encoders
from email.message import Message
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate



from lxml import etree
import cx_Oracle

from dq_scheduler import get_module_logger

logger = get_module_logger(__name__)



class DQJobs(object):
    """
        DQJobs Class that encapsulates all methods and attribute to
        take XML assessment files and correction logs from DQ to 
        the DQ reporting DB. 
    """

    QCSUMMARY_PATH = r'\\path\QCSummary.xml'
    logger.info('QCSummary file: %s', QCSUMMARY_PATH)
       

    
    def __init__(self, XML_SOURCE_DIR, LOG_SOURCE_DIR, DESTINATION_DIR, DAILY_LOG_DIR, SCHEDULED_JOBS_DIR, CONF_FILE_PATH):
        logger.info('Initialising Instance....')
        self.XML_SOURCE_DIR     = XML_SOURCE_DIR
        self.LOG_SOURCE_DIR     = LOG_SOURCE_DIR
        self.DESTINATION_DIR    = DESTINATION_DIR
        self.DAILY_LOG_DIR      = DAILY_LOG_DIR
        self.SCHEDULED_JOBS_DIR = SCHEDULED_JOBS_DIR
        self.username, self.password, self.dbname = self.connect_conf(CONF_FILE_PATH)

        
    def make_namedtuple_factory(self, cursor):
        """
            Create a named tuple from a database cursor
        """
        
        columnNames = [d[0].lower() for d in cursor.description]
        row = collections.namedtuple('row', columnNames)
        return row

    def clear_destination_dir (self):
        """
            Clear all files from previous processing
            in destination direcoty.
        """
        
        for path, dirs, files in os.walk(self.DESTINATION_DIR, topdown=False):
            for file in files:
                os.remove(os.path.join(path, file))

    def copy_renamed_files_to_destination_dir (self, job_run_ids, qcsync_dirs, filter_date):
        """
            copy and rename all XML assessement files and
            correction logs from source directory to destination directory.
        """
        
        # XML assesment files
        assessment_runs = [self.XML_SOURCE_DIR + job_run_id for job_run_id in job_run_ids]
        for path, dirs, files in chain.from_iterable(os.walk(assessment_run) 
                                                      for assessment_run in assessment_runs):
            logger.debug('XML assessment files that need to copied and renamed:')
            for file in files:
                if file not in ['Runs.xml', 'Run.xml']:
                    file_path = os.path.join(path, file)
                    logger.debug(file_path)
                    # Get last modified date for file
                    file_date = datetime.fromtimestamp(os.path.getmtime(os.path.join(path, file)))
                    # Deconstruct the file path into separate elements using '\' as separator
                    file_path_split = file_path.split('\\')
                    # Reconstruct filename with some of these elements
                    new_file_path = file_path_split[-4] + "#" + file_path_split[-3] + file_path_split[-2] + file[:-4] +  "_" + file_date.strftime('%m%d%Y_%H%M_%S') + ".xml"
                    shutil.copy(file_path, self.DESTINATION_DIR + "\\" + new_file_path)
                    
        # Correction logs           
        correction_logs=[self.LOG_SOURCE_DIR + qcsync_dir for qcsync_dir in qcsync_dirs]
        for path, dirs, files in chain.from_iterable(os.walk(correction_log) 
                                                      for correction_log in correction_logs):
            logger.debug('Correction logs that need to copied and renamed:')
            for file in files:
                file_path = os.path.join(path, file)
                logger.debug(file_path)
                # Get last modified date for file
                file_date = datetime.fromtimestamp(os.path.getmtime(os.path.join(path, file)))
                if file_date.date() >= filter_date:
                    shutil.copy(file_path, self.DESTINATION_DIR + "\\" + correction_log + file)


    def get_qcsummary_jobs(self):
        """
            Extract all the jobs defined in QCSummary XML file.
        """
        
        # Parse XML file
        qcsummary_xml = etree.parse(self.QCSUMMARY_PATH)
        qcsummary_jobs=[]
        
        for level in qcsummary_xml.xpath("//Level[@BackOffice='database\\schema']"):
            qcsummary_jobs = qcsummary_jobs + (level.xpath("./@Name"))
            
        logger.info('# QCSummary jobs: %s', len(qcsummary_jobs))
        logger.debug('QCSummary jobs: %s', qcsummary_jobs)
        
        return qcsummary_jobs

    def get_scheduled_jobs(self, filter_date):
        """
            Get all scheduled jobs from the DQ Scheduler that
            were run on the date specified by filter_date parameter.
        """
        
        try:
            con = cx_Oracle.connect(self.username,self.password, self.dbname)
            logger.info('Connected to DB to get ran scheduled jobs')
        except:
            raise
        try:    
            cur = con.cursor()
            logger.debug('Cursor acquired')
            sql = """\
                SELECT column1, column2, column3
                    FROM monitor_schedule m
                WHERE calendar_date >= TO_DATE(:param_date, 'YYYYMMDD')
                AND REGEXP_SUBSTR(schedule_status, '^\w+') NOT IN ('READY','DEACTIVATED')
                AND NOT EXISTS
                    (SELECT null FROM fileupload f WHERE f.runid = m.runid)"""
            cur.execute(sql, param_date = filter_date.strftime('%Y%m%d'))
            # Replace default rowfactory with named tuple.
            cur.rowfactory = self.make_namedtuple_factory(cur)
            scheduled_jobs = cur.fetchall()
            
            logger.info('# Scheduled jobs: %s', len(scheduled_jobs))
            logger.debug('Scheduled jobs: %s', scheduled_jobs)
            
            return scheduled_jobs
            
        except:
            raise
        finally:     
            cur.close()
            con.close()
            
    def schedule_complete (self, scheduled_jobs):
        """
            Check if any jobs did not complete (ran)
        """
        if  any (job.schedule_status =='RUNNING' or job.schedule_status == 'QUEUED' or job.schedule_status == 'NOT RUN' for job in scheduled_jobs):
            logger.info('Schedule Complete: %s', 'False')
            return False
        logger.info('Schedule Complete: %s', 'True')    
        return True

    def get_completed_qcsummary_jobs(self, scheduled_jobs, qcsummary_jobs):
        """
            Get all completed scheduled that have been defined in QCSummary
            A job defined in QCsummary will generate XML Assessment files and
            correction logs to capture results of the job.
        """
        completed_qcsummary_jobs = []
        
        if scheduled_jobs and qcsummary_jobs:
            #Find jobs that have completed and are in QCsummary.
            completed_qcsummary_jobs = [scheduled_job for scheduled_job in scheduled_jobs 
                                        if scheduled_job.aoiname in qcsummary_jobs  and scheduled_job.schedule_status.startswith('RAN')]
                                    
        logger.info('# Completed QCSummary jobs: %s', len(completed_qcsummary_jobs))
        logger.debug('Completed QCSummary jobs: %s', completed_qcsummary_jobs)
        
        return completed_qcsummary_jobs

    def get_runids(self, completed_qcsummary_jobs):
        """
            Get a list of runid for all the jobs that have
            completed succesfully and are defined in QCSummary.
        """
        job_runids = []
        
        if completed_qcsummary_jobs:
            job_runids = [str(completed_qcsummary_job.jobid) + "\\" + str(completed_qcsummary_job.runid)
                        for completed_qcsummary_job in completed_qcsummary_jobs]
                      
        logger.info('# jobid/runid that need to be uploaded: %s', len(job_runids))
        logger.debug('jobid/runid: %s', job_runids)
        
        return job_runids

    def get_qcsyncs_dir(self, completed_qcsummary_jobs):
        """
            Get a list of QCSync folders for all the jobs that have
            completed succesfully and are defined in QCSummary.
        """
        qcsync_dirs = []
        
        if completed_qcsummary_jobs:
            qcsync_dirs = [ "QCSync" + str(completed_qcsummary_job.aoiname) 
                        for completed_qcsummary_job in completed_qcsummary_jobs]
                       
        logger.info('# QCSync folders: %s', len(qcsync_dirs))
        logger.debug('QCSyncs: %s', qcsync_dirs)
        
        return qcsync_dirs

    def upload_assessment_to_db(self):
        """
            Execute dbpk_etl.dbp_process_run in the DQ Reporting
            database to upload XML assessment files and correction logs
        """
        
        try:
            con = cx_Oracle.connect(self.username,self.password, self.dbname)
            logger.info('Connected to DB to upload assessments')
        except:
            raise
            
        try:
            cur = con.cursor()
            logger.debug('Cursor acquired')
            
            # Upload the assessment results to DB fro each runid
            cur.callproc("dbpk_etl.dbp_process_run")
            logger.info('Called dbpk_etl.dbp_process_run')
            
            # Check the upload flag for each runid
            sql = """\
                SELECT runid, DECODE(status, 'Processed', 'Y', 'N') uploaded_flag
                    FROM fileupload 
                WHERE upload_date >= TRUNC(SYSDATE)
                GROUP BY runid, status"""
            cur.execute(sql)
            # Replace default rowfactory with named tuple.
            cur.rowfactory = self.make_namedtuple_factory(cur)
            uploaded_jobs = cur.fetchall()         
            logger.info('# Uploaded jobs: %s', len(uploaded_jobs))
            logger.info('Uploaded jobs: %s', uploaded_jobs)
            
            return uploaded_jobs
              
        except cx_Oracle.DatabaseError as e:
            logger.info('Failed to upload ' + format(e))
        finally:     
            cur.close()
            con.close()
    
    def refresh_mvw_well_failure(self):
        """
            Refresh materialised view MVW_WELL_FAILURE in DQ reporting
        """
        
        try:
            con = cx_Oracle.connect(self.username,self.password, self.dbname)
            logger.info('Connected to DB to refresh materialized view MW_WELL_FAILURE')
        except:
            raise
        try:    
            cur = con.cursor()
            logger.debug('Cursor acquired')
            
            cur.callproc("dbms_mview.refresh",['MVW_WELL_FAILURE','C'])
            logger.info('Called dbms_mview.refresh')
            
        except:
            raise
        finally:     
            cur.close()
            con.close()
            
    
            
    def list_monitored_jobs (self, scheduled_jobs, qcsummary_jobs, uploaded_jobs, last_run_logs):
        """
            Create a list with all the jobs and their details to be used for reporting
        """
        monitored_jobs = []
        
        if scheduled_jobs:
            logger.debug('Adding the QCSummary flag to monitored jobs')
            monitored_jobs = ([scheduled_job + tuple(["Y"]) for scheduled_job in scheduled_jobs if scheduled_job.aoiname in qcsummary_jobs] + 
                            [scheduled_job + tuple(["N"]) for scheduled_job in scheduled_jobs if scheduled_job.aoiname not in qcsummary_jobs])
            # Create a namedtuple as a container for the monitored jobs
            monitored_job_rec = collections.namedtuple('monitored_job_rec', scheduled_jobs[0]._fields  + ('qcsummary_flag',))
            monitored_jobs = map(monitored_job_rec._make, monitored_jobs)
            logger.info('# Monitored jobs: %s', len(monitored_jobs))
        
            logger.debug('Adding the uploaded flag to monitored jobs')
            # Create a namedtuple as a container for the monitored jobs
            monitored_job_rec = collections.namedtuple('monitored_job_rec', monitored_jobs[0]._fields  + ('uploaded_flag',))
            monitored_jobs = ([monitored_job + tuple([uploaded_job.uploaded_flag]) for monitored_job in monitored_jobs for uploaded_job in uploaded_jobs if monitored_job.runid == uploaded_job.runid] + 
                          [monitored_job + tuple(["N/A"]) for monitored_job in monitored_jobs if monitored_job.runid not in [uploaded_job.runid for uploaded_job in uploaded_jobs]])

            monitored_jobs = map(monitored_job_rec._make, monitored_jobs)
            logger.info('# Monitored jobs: %s', len(monitored_jobs))
        
            logger.debug('Adding the last run log hyperlink  to monitored jobs')
            # Create a namedtuple as a container for the monitored jobs
            monitored_job_rec = collections.namedtuple('monitored_job_rec', monitored_jobs[0]._fields  + ('last_run_log_hyperlink',))
            monitored_jobs = ([monitored_job + tuple([last_run_log[1]]) for monitored_job in monitored_jobs for last_run_log in last_run_logs if monitored_job.aoiname == last_run_log[0]] + 
                          [monitored_job + tuple(["No Log Available"]) for monitored_job in monitored_jobs if monitored_job.aoiname not in [last_run_log[0] for last_run_log in last_run_logs]])
            monitored_jobs = map(monitored_job_rec._make, monitored_jobs)
        
        logger.info('# Monitored jobs: %s', len(monitored_jobs))
        logger.debug('Monitored jobs: %s', monitored_jobs)
             
        return monitored_jobs
        
    def generate_report_csv(self, monitored_jobs, filter_date):
        """
            Generate a CSV file for reporting purpose
        """
        output_file_path = None
        
        if monitored_jobs:
            sorted_monitored_jobs = sorted(monitored_jobs, key=operator.attrgetter('grouppath','startdate'))
            output_file_path = self.SCHEDULED_JOBS_DIR + "\\" + "DQ_QCScheduler_Jobs_" + filter_date.strftime('%m%d%Y') + ".csv"
            
            try:
                with open(output_file_path, "w") as output_file:
                    writer = csv.writer(output_file)
                    header_row = [col.upper() for col in sorted_monitored_jobs[0]._fields]
                    writer.writerow(header_row)
                    writer.writerows(sorted_monitored_jobs)
            except IOError as e:
                raise
                logger.info('I/O error(%s): %s', e.errno, e.strerror)
            except:
                raise
                logger.info('Unexpected error: %s', sys.exc_info()[0])

            
        logger.info('Report CSV: %s', output_file_path)
        
        return output_file_path
            
    def get_jobs_summary (self, monitored_jobs):
        """
            Get a count of jobs group by status + last_message
        """
        jobs_summary = collections.Counter()
        # This create a dictionary
        for monitored_job in monitored_jobs:
            if monitored_job.schedule_status == 'RAN' and 'ERROR' not in monitored_job.lastmessage:
                derived_status = 'RAN SUCCESS'
            elif (monitored_job.lastmessage is not None  and 'ERROR' in monitored_job.lastmessage):
                derived_status = 'RAN WITH ERROR(s)'
            elif monitored_job.schedule_status == 'NOT RUN':
                derived_status = 'NOT RUN'
            else:
                derived_status = monitored_job.schedule_status
            
            jobs_summary[derived_status] += 1
        #uploaded_job.uploaded_flag]) for monitored_job in monitored_jobs for uploaded_job in uploaded_jobs if monitored_job.runid == uploaded_job.runid]
        #orderby = ['RAN Completed Run, TOC Mapping Applied', 'RAN Completed Run', 'RAN ERROR(s) - Completed Run, TOC Mapping Applied', 'NOT RUN']
        orderby = ['RAN SUCCESS', 'RAN WITH ERROR(s)', 'RUNNING','QUEUED', 'NOT RUN', 'ABORTED']
              
        jobs_summary_ordered = collections.OrderedDict( (k, jobs_summary[k]) for k in orderby )
                              
        logger.info('# Jobs Summary: %s', len(jobs_summary_ordered))
        logger.info('Jobs Summary: %s', jobs_summary_ordered)
        
        return jobs_summary_ordered
    
    def send_mail(self, send_from, send_to, subject, filter_date, jobs_summary = None, attached_file=None):
        """
            Send email
        """
        
        assert isinstance(send_to, list)

        logger.info('attached_file: %s', attached_file)
        
        msg = MIMEMultipart()
        msg["From"] = send_from
        msg["To"] = COMMASPACE.join(send_to)
        msg["Subject"] = subject
        msg.preamble = subject
        
        
        if jobs_summary:
        
            # Write the email body
            body = filter_date.strftime('DQ QCScheduler status: %d %b %Y.')
            
            body += "\r\n\r\n"
            for keys, values in jobs_summary.items():
                #print(keys)
                #print(values)
                body += "\r\n" + '{:<30}'.format(keys) + ": "  + '{:4d}'.format(values)
            
            body += "\r\n" + '{:<30}'.format('TOTAL') + ": "  + '{:4d}'.format(sum(jobs_summary.itervalues()))
            body
            body_html = """\
            <html>
                <head></head>
                <body>
                    <pre>{body_place_holder}</pre>
                </body>
            </html>
            """
            body_html_complete = body_html.format(body_place_holder=body)
            msg.attach(MIMEText(body_html_complete, 'html'))

        # Check for attachments
        if attached_file is not None:                   
            # Handle attachments
            logger.info('attachment: %s', attached_file)
            ctype, encoding = mimetypes.guess_type(attached_file)
            if ctype is None or encoding is not None:
                ctype = "application/octet-stream"
            maintype, subtype = ctype.split("/", 1)
            if maintype == "text":
                try:
                    with open(attached_file) as email_file:
                    # Note: we should handle calculating the charset
                        attachment = MIMEText(email_file.read(), _subtype=subtype)
                except IOError as e:
                    raise
                    logger.info('I/O error(%s): %s', e.errno, e.strerror)
                except:
                    raise
                    logger.info('Unexpected error: %s', sys.exc_info()[0])
            else:
                try:
                    with open(attached_file, "rb") as email_file:
                        attachment = MIMEBase(maintype, subtype)
                        attachment.set_payload(email_file.read())
                        encoders.encode_base64(attachment)
                except IOError as e:
                    raise
                    logger.info('I/O error(%s): %s', e.errno, e.strerror)
                except:
                    raise
                    logger.info('Unexpected error: %s', sys.exc_info()[0])

            attachment.add_header("Content-Disposition", "attachment", filename=attached_file)
            attachment.add_header("Content-Disposition", 'attachment; filename="{0}"'.format(os.path.basename(attached_file)))
            msg.attach(attachment)
           
        smtp = smtplib.SMTP("emailserver", 25)
        smtp.set_debuglevel(False) # show communication with the server
        try:
            smtp.sendmail(send_from, send_to, msg.as_string())
        finally:
            smtp.quit()
            logger.info('email sent to: %s', send_to)
            
    def extract_last_run_log (self, scheduled_jobs):
        """
            Open DQ log file containing all DQ runs for a particular job and extract logging for last run
        """
        terminate_keywords = {'Exiting QCUpdate', 'Process has been asked to terminate'}
        last_run_logs = []
        
        logger.debug('Extracting DQ internal log for the run')
        for scheduled_job in scheduled_jobs:
        
            if scheduled_job.schedule_status in ['RAN', 'RUNNING', 'ABORTED']:
                QCLog_path = self.LOG_SOURCE_DIR + scheduled_job.aoiname + ".QCLogix.Log"
                logger.debug('QCLog_path: %s', QCLog_path)
                
                try:
                    run_log = open(QCLog_path)
                    
                    last_run_lines=[]
                
                    # starting reading lines from the end of the file
                    for i, line in enumerate(reversed(open(QCLog_path).readlines())):
                        #Read and keep line.
                        last_run_lines.append(line)
                        #Check to see if it's the end of the previous DQ run. 
                        if any(terminate_keyword in line for terminate_keyword in terminate_keywords) and i > 10:
                            #That's the end of previous DQ run, we don't need the line
                            last_run_lines.remove(line)
                            break
                
                    # New file name for last run
                    file_date = datetime.now()
                    last_run_log_name = file_date.strftime('%Y%m%d') + "_" + scheduled_job.aoiname + ".QCLogix.Log"
                    last_run_log_file = open(self.DAILY_LOG_DIR + last_run_log_name, "w")
                    logger.debug('last_run_log_file: %s', last_run_log_file)
                    # wriint extrated last run log to new file
                    for j, last_run_line in enumerate(reversed(last_run_lines)):
                        last_run_log_file.write(last_run_line)
                        
                    last_run_log_file.close()
                    logger.debug('adding ' + last_run_log_name)
                    last_run_logs.append([scheduled_job.aoiname,'=HYPERLINK("file:///' + self.DAILY_LOG_DIR + last_run_log_name + '", "' + last_run_log_name + '")', self.DAILY_LOG_DIR + last_run_log_name])
                    
                except IOError:
                    last_run_logs.append([scheduled_job.aoiname,"Log not found"])
                    logger.info('No Such file: %s', QCLog_path)
                    continue
                
        logger.info('# last_run_logs: %s', len(last_run_logs))
        logger.debug('last_run_logs: %s', last_run_logs)
        
        return last_run_logs
        
    def extract_issues_from_log(self, last_run_logs):
        """
            Identifies "issues" accross the latest DQ internal log for a run with the help of keywords
        """
    
        exception_keywords = {'Error', 'Failed', 'Warning'}
        
        exception_lines = []
        for last_run_log in last_run_logs:
             
            exception_lines = []
            file_date = datetime.now()
            error_last_run_log_name = file_date.strftime('%Y%m%d') + "_" + last_run_log[0] + "_ERROR" + ".QCLogix.Log"
            
            last_run_lines = open(last_run_log[2]).readlines()
        
            for i, line in enumerate(last_run_lines):
        
                #Identify lines with indicating errors, warning, failures, etc...
                if any(exception_keyword in line for exception_keyword in exception_keywords):
                    exception_lines.append(list(range(i-3, i+4)))
                
            #Create a new file with last run lines.	
            last_error_log_file = open(self.DAILY_LOG_DIR + error_last_run_log_name, "w")
            for line_block in exception_lines:
                for i, line in enumerate(line_block):
                    last_error_log_file.write(list(last_run_lines)[line])
                last_error_log_file.write("\n")

            last_error_log_file.close()
        

    def connect_conf(self, conf_file):
        """
            Reads an encrypted string to retrieve credentials
        """
        try:
            #logger.info('conf_file: %s', conf_file)

            f = open(conf_file, 'r')
            encryptedstring = f.readline().strip()
            f.close()

            rawstring = base64.b64decode(encryptedstring)

            p = r'^(.*)/(.*)@(.*)$'
            m = re.search(p, rawstring)

            
            username = m.group(1)
            password = m.group(2)
            dsn = m.group(3)

        except:
            raise

        return  username, password, dsn
    
        
def encrypt_db_credentials(db_credentials, conf_file):
    """
        Encrypt db credentials 
    """

    file = open(conf_file, "w")
    file.write(base64.b64encode(db_credentials) + "\n")
    file.close()