import os
import json
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
from dotenv import load_dotenv
from barcode_sync import send_sql_query
from job_manager import get_job_manager, initialize_jobs

# Load environment variables
load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'your-default-secret-key')

# Fixed credentials from environment
WEB_USERNAME = os.getenv('WEB_USERNAME', 'admin')
WEB_PASSWORD = os.getenv('WEB_PASSWORD', 'd1sapsync2024')

# Initialize background jobs (must be after app creation for logging to work)
initialize_jobs()

def login_required(f):
    """Decorator to require login for protected routes"""
    def decorated_function(*args, **kwargs):
        if not session.get('logged_in'):
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    decorated_function.__name__ = f.__name__
    return decorated_function

@app.route('/')
def index():
    """Redirect to login or query page"""
    if session.get('logged_in'):
        return redirect(url_for('query_page'))
    return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    """Login page"""
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']

        if username == WEB_USERNAME and password == WEB_PASSWORD:
            session['logged_in'] = True
            session['username'] = username
            flash('Successfully logged in!', 'success')
            return redirect(url_for('query_page'))
        else:
            flash('Invalid username or password!', 'error')

    return render_template('login.html')

@app.route('/logout')
def logout():
    """Logout and clear session"""
    session.clear()
    flash('You have been logged out.', 'info')
    return redirect(url_for('login'))

@app.route('/query')
@login_required
def query_page():
    """Main SQL query interface"""
    return render_template('query.html', username=session.get('username'))

@app.route('/execute_query', methods=['POST'])
@login_required
def execute_query():
    """Execute SQL query via API"""
    try:
        query = request.json.get('query', '').strip()

        if not query:
            return jsonify({
                'success': False,
                'error': 'No query provided'
            })

        # Execute the query
        result = send_sql_query(query)

        if result is not None:
            # Convert result to a more manageable format
            if isinstance(result, list) and len(result) > 0:
                # Get columns from first row
                columns = list(result[0].keys()) if result[0] else []

                return jsonify({
                    'success': True,
                    'data': result,
                    'columns': columns,
                    'row_count': len(result),
                    'query': query
                })
            else:
                return jsonify({
                    'success': True,
                    'data': [],
                    'columns': [],
                    'row_count': 0,
                    'query': query,
                    'message': 'Query executed successfully but returned no results'
                })
        else:
            return jsonify({
                'success': False,
                'error': 'Query failed or returned no data',
                'query': query
            })

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'query': request.json.get('query', '')
        })

@app.route('/sample_queries')
@login_required
def sample_queries():
    """Get sample queries"""
    samples = [
        {
            'name': 'Company Information',
            'query': 'SELECT TOP 5 CompnyName, CompnyAddr FROM OADM'
        },
        {
            'name': 'Items with Barcodes',
            'query': 'SELECT TOP 10 ItemCode, ItemName, CodeBars FROM OITM WHERE CodeBars IS NOT NULL'
        },
        {
            'name': 'Alternative Barcodes',
            'query': 'SELECT TOP 10 ItemCode, BcdCode, BcdName FROM OBCD'
        },
        {
            'name': 'Item Groups',
            'query': 'SELECT TOP 5 ItmsGrpCod, ItmsGrpNam FROM OITB'
        },
        {
            'name': 'Business Partners',
            'query': 'SELECT TOP 10 CardCode, CardName, CardType FROM OCRD'
        }
    ]

    return jsonify({'samples': samples})

# Job Management API Endpoints

@app.route('/api/jobs')
@login_required
def get_jobs():
    """Get status of all background jobs"""
    job_manager = get_job_manager()
    jobs = job_manager.get_all_jobs_status()
    return jsonify({'jobs': jobs})

@app.route('/api/jobs/<job_id>')
@login_required
def get_job(job_id):
    """Get status of a specific job"""
    job_manager = get_job_manager()
    job = job_manager.get_job_status(job_id)
    if job:
        return jsonify({'job': job})
    else:
        return jsonify({'error': 'Job not found'}), 404

@app.route('/api/jobs/<job_id>/logs')
@login_required
def get_job_logs(job_id):
    """Get logs for a specific job"""
    job_manager = get_job_manager()
    lines = request.args.get('lines', 100, type=int)
    logs = job_manager.get_job_logs(job_id, lines)
    return jsonify({'logs': logs})

@app.route('/api/jobs/<job_id>/start', methods=['POST'])
@login_required
def start_job(job_id):
    """Start a background job"""
    job_manager = get_job_manager()
    success = job_manager.start_job(job_id)
    if success:
        return jsonify({'message': f'Job {job_id} started successfully'})
    else:
        return jsonify({'error': f'Failed to start job {job_id}'}), 500

@app.route('/api/jobs/<job_id>/stop', methods=['POST'])
@login_required
def stop_job(job_id):
    """Stop a background job"""
    job_manager = get_job_manager()
    success = job_manager.stop_job(job_id)
    if success:
        return jsonify({'message': f'Job {job_id} stopped successfully'})
    else:
        return jsonify({'error': f'Failed to stop job {job_id}'}), 500

@app.route('/api/jobs/<job_id>/restart', methods=['POST'])
@login_required
def restart_job(job_id):
    """Restart a background job"""
    job_manager = get_job_manager()
    success = job_manager.restart_job(job_id)
    if success:
        return jsonify({'message': f'Job {job_id} restarted successfully'})
    else:
        return jsonify({'error': f'Failed to restart job {job_id}'}), 500

@app.route('/jobs')
@login_required
def jobs_page():
    """Job management dashboard"""
    return render_template('jobs.html', username=session.get('username'))

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=9000)