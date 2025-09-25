import subprocess
import threading
import time
import json
import logging
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional
import queue
import os

class JobStatus(Enum):
    STOPPED = "stopped"
    RUNNING = "running"
    FAILED = "failed"
    COMPLETED = "completed"
    SCHEDULED = "scheduled"

class JobManager:
    """
    Background job management system for running and monitoring processes
    """
    def __init__(self):
        self.jobs: Dict[str, Dict] = {}
        self.logger = logging.getLogger(__name__)
        self._lock = threading.Lock()

    def register_job(self, config: Dict):
        """Register a new job type with full configuration"""
        job_id = config['job_id']

        with self._lock:
            self.jobs[job_id] = {
                'name': config.get('name', job_id),
                'command': config['command'],
                'description': config.get('description', ''),
                'auto_restart': config.get('auto_restart', False),
                'restart_delay': config.get('restart_delay', 30),
                'run_interval': config.get('run_interval', 300),
                'auto_start': config.get('auto_start', False),
                'max_restarts': config.get('max_restarts', 5),
                'enabled': config.get('enabled', True),
                'status': JobStatus.STOPPED,
                'process': None,
                'thread': None,
                'scheduler_thread': None,
                'start_time': None,
                'end_time': None,
                'last_run_time': None,
                'next_run_time': None,
                'logs': [],
                'log_queue': queue.Queue(),
                'restart_count': 0,
                'run_count': 0,
                'is_scheduled': config.get('run_interval', 0) > 0
            }

    def start_job(self, job_id: str) -> bool:
        """Start a background job (either scheduled or continuous)"""
        if job_id not in self.jobs:
            self.logger.error(f"Job {job_id} not registered")
            return False

        job = self.jobs[job_id]

        if job['status'] == JobStatus.RUNNING or job['status'] == JobStatus.SCHEDULED:
            self.logger.warning(f"Job {job_id} is already running")
            return True

        try:
            job['restart_count'] = 0  # Reset restart count when manually started

            if job['is_scheduled']:
                # For scheduled jobs, start the scheduler
                return self._start_scheduled_job(job_id)
            else:
                # For continuous jobs, start the process directly
                return self._start_continuous_job(job_id)

        except Exception as e:
            self.logger.error(f"Failed to start job {job_id}: {e}")
            job['status'] = JobStatus.FAILED
            return False

    def _start_scheduled_job(self, job_id: str) -> bool:
        """Start a scheduled job that runs at intervals"""
        job = self.jobs[job_id]

        job['status'] = JobStatus.SCHEDULED
        job['start_time'] = datetime.now()
        job['next_run_time'] = datetime.now()

        # Start scheduler thread
        scheduler_thread = threading.Thread(
            target=self._schedule_job,
            args=(job_id,),
            daemon=True
        )
        job['scheduler_thread'] = scheduler_thread
        scheduler_thread.start()

        self.logger.info(f"Started scheduled job {job_id} (interval: {job['run_interval']}s)")
        return True

    def _start_continuous_job(self, job_id: str) -> bool:
        """Start a continuous job that runs until stopped"""
        job = self.jobs[job_id]

        # Clear old logs if starting fresh
        job['logs'] = []
        job['log_queue'] = queue.Queue()

        # Start the process
        process = subprocess.Popen(
            job['command'],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1
        )

        job['process'] = process
        job['status'] = JobStatus.RUNNING
        job['start_time'] = datetime.now()
        job['end_time'] = None

        # Start monitoring thread
        monitor_thread = threading.Thread(
            target=self._monitor_job,
            args=(job_id,),
            daemon=True
        )
        job['thread'] = monitor_thread
        monitor_thread.start()

        self.logger.info(f"Started continuous job {job_id} (PID: {process.pid})")
        return True

    def _schedule_job(self, job_id: str):
        """Scheduler thread that runs a job at intervals"""
        job = self.jobs[job_id]

        try:
            while job['status'] == JobStatus.SCHEDULED:
                current_time = datetime.now()

                if current_time >= job['next_run_time']:
                    self.logger.info(f"Running scheduled job {job_id}")

                    # Add log entry for job start
                    log_entry = {
                        'timestamp': current_time.isoformat(),
                        'level': 'INFO',
                        'message': f"🚀 Starting scheduled run #{job['run_count'] + 1}"
                    }
                    job['log_queue'].put(log_entry)

                    # Run the job
                    job['last_run_time'] = current_time
                    job['run_count'] += 1
                    success = self._execute_job_run(job_id)

                    # Schedule next run
                    job['next_run_time'] = datetime.now() + timedelta(seconds=job['run_interval'])

                    if not success and job['restart_count'] >= job['max_restarts']:
                        self.logger.error(f"Job {job_id} exceeded max restarts ({job['max_restarts']})")
                        job['status'] = JobStatus.FAILED
                        job['end_time'] = datetime.now()
                        break

                # Sleep for a short interval before checking again
                time.sleep(1)

        except Exception as e:
            self.logger.error(f"Error in scheduler for job {job_id}: {e}")
            job['status'] = JobStatus.FAILED
            job['end_time'] = datetime.now()

    def _execute_job_run(self, job_id: str) -> bool:
        """Execute a single run of a job"""
        job = self.jobs[job_id]

        try:
            # Start the process
            process = subprocess.Popen(
                job['command'],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1
            )

            # Read output and add to logs
            for line in iter(process.stdout.readline, ''):
                if line:
                    log_entry = {
                        'timestamp': datetime.now().isoformat(),
                        'level': 'INFO',
                        'message': line.strip()
                    }
                    job['log_queue'].put(log_entry)

            # Wait for completion
            return_code = process.wait()

            if return_code == 0:
                log_entry = {
                    'timestamp': datetime.now().isoformat(),
                    'level': 'INFO',
                    'message': f"✅ Job run completed successfully"
                }
                job['log_queue'].put(log_entry)
                return True
            else:
                log_entry = {
                    'timestamp': datetime.now().isoformat(),
                    'level': 'ERROR',
                    'message': f"❌ Job run failed with return code {return_code}"
                }
                job['log_queue'].put(log_entry)
                job['restart_count'] += 1
                return False

        except Exception as e:
            log_entry = {
                'timestamp': datetime.now().isoformat(),
                'level': 'ERROR',
                'message': f"❌ Job execution error: {str(e)}"
            }
            job['log_queue'].put(log_entry)
            job['restart_count'] += 1
            return False

    def stop_job(self, job_id: str) -> bool:
        """Stop a running job"""
        if job_id not in self.jobs:
            return False

        job = self.jobs[job_id]

        if job['status'] not in [JobStatus.RUNNING, JobStatus.SCHEDULED]:
            return True

        try:
            # Stop any running process
            if job['process']:
                job['process'].terminate()
                # Wait a bit for graceful shutdown
                try:
                    job['process'].wait(timeout=5)
                except subprocess.TimeoutExpired:
                    # Force kill if it doesn't terminate gracefully
                    job['process'].kill()
                    job['process'].wait()

            job['status'] = JobStatus.STOPPED
            job['end_time'] = datetime.now()
            self.logger.info(f"Stopped job {job_id}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to stop job {job_id}: {e}")
            return False

    def restart_job(self, job_id: str) -> bool:
        """Restart a job"""
        self.stop_job(job_id)
        time.sleep(1)  # Brief pause
        return self.start_job(job_id)

    def get_job_status(self, job_id: str) -> Optional[Dict]:
        """Get current status of a job"""
        if job_id not in self.jobs:
            return None

        job = self.jobs[job_id]
        return {
            'job_id': job_id,
            'name': job['name'],
            'description': job['description'],
            'status': job['status'].value,
            'is_scheduled': job['is_scheduled'],
            'run_interval': job['run_interval'],
            'start_time': job['start_time'].isoformat() if job['start_time'] else None,
            'end_time': job['end_time'].isoformat() if job['end_time'] else None,
            'last_run_time': job['last_run_time'].isoformat() if job['last_run_time'] else None,
            'next_run_time': job['next_run_time'].isoformat() if job['next_run_time'] else None,
            'restart_count': job['restart_count'],
            'run_count': job['run_count'],
            'max_restarts': job['max_restarts'],
            'auto_restart': job['auto_restart'],
            'enabled': job['enabled'],
            'pid': job['process'].pid if job['process'] and job['status'] == JobStatus.RUNNING else None,
            'log_count': len(job['logs'])
        }

    def get_all_jobs_status(self) -> List[Dict]:
        """Get status of all registered jobs"""
        return [self.get_job_status(job_id) for job_id in self.jobs.keys()]

    def get_job_logs(self, job_id: str, lines: int = 100) -> List[Dict]:
        """Get recent logs for a job"""
        if job_id not in self.jobs:
            return []

        job = self.jobs[job_id]

        # Get any new logs from queue
        while True:
            try:
                log_entry = job['log_queue'].get_nowait()
                job['logs'].append(log_entry)
            except queue.Empty:
                break

        # Return most recent logs
        return job['logs'][-lines:] if job['logs'] else []

    def _monitor_job(self, job_id: str):
        """Monitor a running job and capture its output"""
        job = self.jobs[job_id]
        process = job['process']

        try:
            # Read output line by line
            for line in iter(process.stdout.readline, ''):
                if line:
                    log_entry = {
                        'timestamp': datetime.now().isoformat(),
                        'level': 'INFO',
                        'message': line.strip()
                    }
                    job['log_queue'].put(log_entry)

            # Wait for process to complete
            return_code = process.wait()

            # Update job status based on return code
            if return_code == 0:
                job['status'] = JobStatus.COMPLETED
                self.logger.info(f"Job {job_id} completed successfully")
            else:
                job['status'] = JobStatus.FAILED
                self.logger.error(f"Job {job_id} failed with return code {return_code}")

            job['end_time'] = datetime.now()

            # Handle auto-restart
            if job['auto_restart'] and job['status'] == JobStatus.FAILED:
                self.logger.info(f"Auto-restarting job {job_id} in {job['restart_delay']} seconds")
                time.sleep(job['restart_delay'])
                job['restart_count'] += 1
                self.start_job(job_id)

        except Exception as e:
            self.logger.error(f"Error monitoring job {job_id}: {e}")
            job['status'] = JobStatus.FAILED
            job['end_time'] = datetime.now()

# Global job manager instance
job_manager = JobManager()

def initialize_jobs():
    """Initialize all jobs from configuration"""
    from job_config import JobConfig

    # Register all configured jobs
    for config in JobConfig.get_all_job_configs():
        job_manager.register_job(config)

        # Auto-start jobs if configured
        if config.get('auto_start', False):
            job_manager.start_job(config['job_id'])

def get_job_manager():
    """Get the global job manager instance"""
    return job_manager