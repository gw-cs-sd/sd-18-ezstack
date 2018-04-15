from flask import Flask, jsonify, request
from tempfile import NamedTemporaryFile
from ruamel import yaml

import argparse
import subprocess

app = Flask(__name__)

bootstrap_properties = {}
run_app_path = ''

@app.route('/deploy/bootstrap/<job_id>', methods=['GET'])
def deploy_job(job_id):
    properties = {}
    for key in bootstrap_properties.keys():
        properties[key] = bootstrap_properties[key]
    properties['job.id'] = job_id

    f = NamedTemporaryFile(delete=False, suffix='.properties', prefix='ezstack-bootstrapper-', mode='w+')
    for key in properties.keys():
        f.write('{}={}\n'.format(key, properties[key]))
    f.close()

    cmd = [
        run_app_path,
        '--config-factory=org.apache.samza.config.factories.PropertiesConfigFactory',
        '--config-path=file://{}'.format(f.name)
    ]

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (stdout, stderr) = proc.communicate()
    status = proc.returncode

    success = True
    status_code = 201

    if status != 0:
        success = False
        status_code = 400

    response = {
        'job.id' : job_id,
        'success' : success,
        'properties' : properties,
        'stdout' : str(stdout),
        'stderr' : str(stderr)
    }

    return jsonify(response), status_code



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Bootstrapper Webserver')
    parser.add_argument('--port', default=8000, type=int, help='port to listen on')
    parser.add_argument('-p', '--path', dest='path', help='specifies properties path', required=True)
    parser.add_argument('-rp', '--rpath', dest='run_path', help='specifies the run-app.sh path', required=True)
    args = parser.parse_args()
    bootstrap_properties = yaml.safe_load(open(args.path, 'r'))
    run_app_path = args.run_path
    app.run(host='0.0.0.0', port=args.port)