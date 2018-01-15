"""
********************************************************************************
Description:
This module monitors overnight DQ automated jobs and uploads their assessment
to the DQ Reporting database

Ver   Date         Author                    Description
---   --------     -----------------------   ----------------------------------
0.1   22-FEB-2016  Victor Saraiva            Initial Version
********************************************************************************
"""

from datetime import datetime, timedelta
import logging
import csv

import dq_utils

def get_module_logger(mod_name):
    """
        Initialise and format logging capability
    """
    logger = logging.getLogger(mod_name)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger

def main():
    """
        Steps through all the tasks required to upload DQ assessment results into DQ reporting database
    """

    # These configuration settings are for Production
    XML_SOURCE_DIR =  r'\\path\xml\\'
    logger.info('XML source folder: %s', XML_SOURCE_DIR)
    
    LOG_SOURCE_DIR  =  r'\\path\logs\\'
    logger.info('Log folder: %s', LOG_SOURCE_DIR)
    
    DESTINATION_DIR = r'\\path\DQ\\'
    logger.info('Destination folder: %s', DESTINATION_DIR)
    
    DAILY_LOG_DIR  =  r'\\path\logs\Daily\\'
    logger.info('Daily Log folder: %s', DAILY_LOG_DIR)
    
    SCHEDULED_JOBS_DIR = r'\\path\scheduled_jobs\\'
    logger.info('Scheduled Jobs folder: %s', SCHEDULED_JOBS_DIR)
    
    CONF_FILE_PATH = 'conf_file.txt'
    
    filter_date = datetime.now().date() - timedelta(days=1)
    logger.info('date for filter: %s', filter_date.strftime('%Y%m%d'))
    
    # instantiate DQJobs class
    overnight_jobs = dq_utils.DQJobs(XML_SOURCE_DIR, LOG_SOURCE_DIR, DESTINATION_DIR, DAILY_LOG_DIR, SCHEDULED_JOBS_DIR, CONF_FILE_PATH)
    
    # Go to BackOffice and check what jobs were scheduled
    scheduled_jobs = overnight_jobs.get_scheduled_jobs(filter_date)    
        
    # What jobs need to generate QC Summary XML files   
    qcsummary_jobs = overnight_jobs.get_qcsummary_jobs()
    
    # Identify jobs that have completed and have generated QC Summary XML files
    # as their XML files need to be uploaded to the database
    completed_qcsummary_jobs = overnight_jobs.get_completed_qcsummary_jobs(scheduled_jobs, qcsummary_jobs)
    
    # For these jobs get their runid
    job_runids = overnight_jobs.get_runids(completed_qcsummary_jobs)
    
    # Get their correction log folder
    qcsyncs_dirs = overnight_jobs.get_qcsyncs_dir(completed_qcsummary_jobs)
    
    # XML files and Correction logs need to renamed and moved
    # to a network drive accessible by the DQ reporting database
    overnight_jobs.clear_destination_dir()
    overnight_jobs.copy_renamed_files_to_destination_dir(job_runids, qcsyncs_dirs, filter_date)
    
    # Upload files to DQ reporting database
    uploaded_jobs = overnight_jobs.upload_assessment_to_db()
    if uploaded_jobs:
        overnight_jobs.refresh_mvw_well_failure()
        
    # Extract Internal DQ log for run
    last_run_logs = overnight_jobs.extract_last_run_log(scheduled_jobs)
    
    # Prepare detailed list of scheduled jobs and create CSV with it
    monitored_jobs = overnight_jobs.list_monitored_jobs(scheduled_jobs, qcsummary_jobs, uploaded_jobs, last_run_logs)
    output_file_path = overnight_jobs.generate_report_csv(monitored_jobs, filter_date)
    # Create a high-level summary
    jobs_summary_ordered = overnight_jobs.get_jobs_summary(monitored_jobs)
    # Notify if any of the scheduled jobs has not completed
    if not overnight_jobs.schedule_complete(scheduled_jobs):
        email_subject = 'DQ QCScheduler - WARNING'
    else:
        email_subject = 'DQ QCScheduler - STATUS'
    
    # Send CSV and high-level summary by email
    overnight_jobs.send_mail('mailbox@email.com', ['victor.saraiva@email.com', 'mailbox@email.com'], email_subject, filter_date, jobs_summary_ordered, output_file_path)
    


if __name__ == '__main__':
    logger = get_module_logger(__name__)
    main()