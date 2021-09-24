import json
from tapipy.tapis import Tapis

t = Tapis(base_url='https://dev.tapis.io', username='testuser3', password='testuser3')
t.get_tokens()

import os
os.environ['TAPIS_PIPELINES_CONFIG_FILE_PATH'] = '/home/jstubbs/gits/pipelines/samples/tapis_word_stats/word_stats_pipeline_config.json'


def register_systems():
    exec_sys = json.load(open('examples/tapis_exec_system.json', 'r'))
    t.systems.createSystem(**exec_sys)
    remote_outbox = json.load(open('tapis_remote_outbox_storage_system.json', 'r'))
    t.systems.createSystem(**remote_outbox)
    remote_inbox = json.load(open('tapis_remote_inbox_storage_system.json', 'r'))
    t.systems.createSystem(**remote_inbox)
    local_inbox = json.load(open('tapis_local_inbox_storage_system.json', 'r'))
    t.systems.createSystem(**local_inbox)

def update_exec_system():
    exec_sys = json.load(open('examples/tapis_exec_system.json', 'r'))
    t.systems.putSystem(systemId='word_stats.pipelines.example.exec', **exec_sys)

def register_app():
    app = json.load(open('app.json', 'r'))
    t.apps.createAppVersion(**app)

def submit_test_job():
    job = json.load(open('examples/job_example.json', 'r'))
    t.jobs.submitJob(**job)

