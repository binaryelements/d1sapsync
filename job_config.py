import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class JobConfig:
    """Configuration settings for different background jobs"""

    @staticmethod
    def get_barcode_sync_config():
        """Get configuration for barcode sync job"""
        return {
            'job_id': 'barcode_sync',
            'name': 'Barcode Sync',
            'command': ['python', 'barcode_sync.py'],
            'description': 'Synchronizes barcodes from SAP B1 to MySQL database',
            'auto_restart': os.getenv('BARCODE_SYNC_AUTO_RESTART', 'true').lower() == 'true',
            'restart_delay': int(os.getenv('BARCODE_SYNC_RESTART_DELAY', '60')),
            'run_interval': int(os.getenv('BARCODE_SYNC_INTERVAL', '300')),  # Run every 5 minutes by default
            'auto_start': os.getenv('BARCODE_SYNC_AUTO_START', 'true').lower() == 'true',
            'max_restarts': int(os.getenv('BARCODE_SYNC_MAX_RESTARTS', '5')),
            'enabled': os.getenv('BARCODE_SYNC_ENABLED', 'true').lower() == 'true'
        }

    @staticmethod
    def get_serial_number_sync_config():
        """Configuration for serial number sync job"""
        return {
            'job_id': 'serial_number_sync',
            'name': 'Serial Number Sync',
            'command': ['python', 'serial_number_sync.py'],
            'description': 'Synchronizes serial number requirements from SAP B1 to product_associated_details table',
            'auto_restart': os.getenv('SERIAL_SYNC_AUTO_RESTART', 'true').lower() == 'true',
            'restart_delay': int(os.getenv('SERIAL_SYNC_RESTART_DELAY', '60')),
            'run_interval': int(os.getenv('SERIAL_SYNC_INTERVAL', '900')),  # Run every 15 minutes
            'auto_start': os.getenv('SERIAL_SYNC_AUTO_START', 'true').lower() == 'true',
            'max_restarts': int(os.getenv('SERIAL_SYNC_MAX_RESTARTS', '5')),
            'enabled': os.getenv('SERIAL_SYNC_ENABLED', 'true').lower() == 'true'
        }

    @staticmethod
    def get_staff_sync_config():
        """Configuration for staff sync job"""
        return {
            'job_id': 'staff_sync',
            'name': 'Staff Sync',
            'command': ['python', 'staff_sync.py'],
            'description': 'Synchronizes staff/salesperson data from SAP B1 OSLP to app_users table',
            'auto_restart': os.getenv('STAFF_SYNC_AUTO_RESTART', 'true').lower() == 'true',
            'restart_delay': int(os.getenv('STAFF_SYNC_RESTART_DELAY', '60')),
            'run_interval': int(os.getenv('STAFF_SYNC_INTERVAL', '7200')),  # Run every 2 hours
            'auto_start': os.getenv('STAFF_SYNC_AUTO_START', 'false').lower() == 'true',
            'max_restarts': int(os.getenv('STAFF_SYNC_MAX_RESTARTS', '5')),
            'enabled': os.getenv('STAFF_SYNC_ENABLED', 'true').lower() == 'true'
        }

    @staticmethod
    def get_sample_job_config():
        """Example configuration for another job"""
        return {
            'job_id': 'sample_job',
            'name': 'Sample Job',
            'command': ['python', 'sample_job.py'],
            'description': 'Sample background job for demonstration',
            'auto_restart': os.getenv('SAMPLE_JOB_AUTO_RESTART', 'false').lower() == 'true',
            'restart_delay': int(os.getenv('SAMPLE_JOB_RESTART_DELAY', '30')),
            'run_interval': int(os.getenv('SAMPLE_JOB_INTERVAL', '600')),  # Run every 10 minutes
            'auto_start': os.getenv('SAMPLE_JOB_AUTO_START', 'false').lower() == 'true',
            'max_restarts': int(os.getenv('SAMPLE_JOB_MAX_RESTARTS', '3')),
            'enabled': os.getenv('SAMPLE_JOB_ENABLED', 'false').lower() == 'true'
        }

    @staticmethod
    def get_all_job_configs():
        """Get all job configurations"""
        configs = []

        # Add barcode sync if enabled
        barcode_config = JobConfig.get_barcode_sync_config()
        if barcode_config['enabled']:
            configs.append(barcode_config)

        # Add serial number sync if enabled
        serial_config = JobConfig.get_serial_number_sync_config()
        if serial_config['enabled']:
            configs.append(serial_config)

        # Add staff sync if enabled
        staff_config = JobConfig.get_staff_sync_config()
        if staff_config['enabled']:
            configs.append(staff_config)

        # Add other jobs as needed
        # sample_config = JobConfig.get_sample_job_config()
        # if sample_config['enabled']:
        #     configs.append(sample_config)

        return configs