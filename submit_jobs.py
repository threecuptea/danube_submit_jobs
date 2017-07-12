__author__ = 'dev'

import argparse
import base64
import httplib
import json
import mysql.connector
import os
import os.path
import psutil
import re
import subprocess
import sys


APP_PROPERTIES = '/opt/danube/conf/danube-apps.properties'
ENV_PROPERTIES = '/opt/danube/conf/danube-apps-env.properties'

PROP_SUBMIT_APP_DIR = 'submit.app.directory'

PROP_SUBMIT_BROKER_COUNT_MAX = 'submit.job.broker.msg.count.max'
PROP_SUBMIT_MQ_MEMORY_USED_MAX = 'submit.job.mq.memory.used.max'
PROP_SUBMIT_HOST_MEMORY_FREE_MIN = 'submit.job.host.memory.free.min'

DEFAULT_SUBMIT_BROKER_COUNT_MAX = 1000000
DEFAULT_SUBMIT_MQ_MEMORY_USED_MAX = 2.0 #2.0G used
DEFAULT_SUBMIT_HOST_MEMORY_FREE_MIN = 5.0 # At least 5GB

DEFAULT_MQ_ADMIN_PORT = 15672

PROP_BROKER_URL = 'broker.mysql.url'
PROP_BROKER_USER = 'broker.mysql.username'
PROP_BROKER_PASS = 'broker.mysql.password'

PROP_MQ_HOST = 'danube.rabbit.host'
PROP_MQ_USER = 'danube.rabbit.username'
PROP_MQ_PASS = 'danube.rabbit.password'

GB_TO_BYTES = 1024**3



def get_mysql_connection():
  """
  Get a valid mysql cursor
  :return: mysql cursor to be re-used
  """
  global java_props

  m = re.match('jdbc:mysql://(.*)/(.*)', java_props[PROP_BROKER_URL])
  if m:
      host = m.group(1)
      database = m.group(2)
      user_name = java_props[PROP_BROKER_USER]
      password = java_props[PROP_BROKER_PASS]
      conn = mysql.connector.connect(user=user_name, password=password, host=host, database=database)
      # mysql_conn.start_transaction(isolation_level='READ UNCOMMITTED')
      return conn
  else:
    print '%s is not well defined' % PROP_BROKER_URL
    sys.exit(-1)


def get_mq_connection():
  global java_props

  host = java_props[PROP_MQ_HOST]
  username = java_props[PROP_MQ_USER]
  password = java_props[PROP_MQ_PASS]
  auth = 'Basic ' + base64.b64encode(username + ':' + password)
  conn = httplib.HTTPConnection(host, DEFAULT_MQ_ADMIN_PORT, True, 30)
  conn.connect()

  return conn, auth

def parse_validate_java_properties(paths):
  """
  :param paths: properties file path
  :return: dictionary of key, value pair
  """
  props = {}
  for path in paths:
    if os.path.isfile(path):
        with open(path, 'r', 0x4000) as f:
          for line in f:
            if line[0] in ['#', '!']:
              continue
            line = line.rstrip()
            if line == '':
              continue
            key, value = re.split('\s*=\s*', line, 1)
            props[key] = value
  validate_properties(props)
  return props

def validate_properties(props):
  if PROP_BROKER_URL not in props:
    raise ValueError('%s is not defined in danube properties' % PROP_BROKER_URL)
  elif PROP_BROKER_USER not in props:
    raise ValueError('%s is not defined in danube properties' % PROP_BROKER_USER)
  elif PROP_BROKER_PASS not in props:
    raise ValueError('%s is not defined in danube properties' % PROP_BROKER_PASS)
  elif PROP_MQ_HOST not in props:
    raise ValueError('%s is not defined in danube properties' % PROP_MQ_HOST)
  elif PROP_MQ_USER not in props:
    raise ValueError('%s is not defined in danube properties' % PROP_MQ_USER)
  elif PROP_MQ_PASS not in props:
    raise ValueError('%s is not defined in danube properties' % PROP_MQ_PASS)
  elif PROP_SUBMIT_APP_DIR not in props:
    raise ValueError('%s is not defined in danube properties' % PROP_SUBMIT_APP_DIR)


def broker_available():
  global java_props
  global mysql_conn

  max_msg_count = int(java_props[PROP_SUBMIT_BROKER_COUNT_MAX]) if PROP_SUBMIT_BROKER_COUNT_MAX in java_props else DEFAULT_SUBMIT_BROKER_COUNT_MAX

  cursor = mysql_conn.cursor(dictionary=True, buffered=True)
  cursor.execute('EXPLAIN SELECT count(1) FROM queue')
  result = cursor.fetchone()
  msg_count = int(result['rows'])
  cursor.close()
  # print 'broker messages count: %d, threshold: %d' % (msg_count, max_msg_count)
  return True if msg_count < max_msg_count else False


def mq_available():
  global java_props

  max_mem_used_gb = float(java_props[PROP_SUBMIT_MQ_MEMORY_USED_MAX]) if PROP_SUBMIT_MQ_MEMORY_USED_MAX in java_props else DEFAULT_SUBMIT_MQ_MEMORY_USED_MAX
  conn, auth = get_mq_connection()
  conn.request('GET', '/api/nodes', headers={'authorization': auth})
  resp = conn.getresponse()
  if resp.status == 200:
    node = json.loads(resp.read())[0]
    mem_used = node['mem_used']
    # print 'MQ memory used: %d, threshold: %d' % (mem_used, max_mem_used_gb * GB_TO_BYTES)
    conn.close()
    return True if mem_used < max_mem_used_gb * GB_TO_BYTES else False # do I need to convert
  else:
    conn.close()
    print 'Unable to get valid response from %s:%s' % (conn.host, conn.port)
    sys.exit(-1)


def host_available():
  global java_props

  min_mem_free_gb = float(java_props[PROP_SUBMIT_HOST_MEMORY_FREE_MIN]) if PROP_SUBMIT_HOST_MEMORY_FREE_MIN in java_props else DEFAULT_SUBMIT_HOST_MEMORY_FREE_MIN
  svmem = psutil.virtual_memory()
  # print 'Host free memory: %d, threshold: %d' % (svmem.free, min_mem_free_gb * GB_TO_BYTES)
  return True if svmem.free > min_mem_free_gb * GB_TO_BYTES else False


def get_next_submit_job():
  global mysql_conn

  cursor = mysql_conn.cursor(dictionary=True, buffered=True)
  cursor.execute('SELECT id, submit_statement FROM submit_jobs WHERE disabled = 0 AND start_ts IS NULL ORDER BY override desc, id asc LIMIT 1')
  result = cursor.fetchone()
  cursor.close()
  if result is not None:
    return result['id'], result['submit_statement'] 
  else:
    return None, None

def submit_next_job(raw_statement, job_id):
  global java_props

  m = re.match('(.*)submit (.*)( &)*', raw_statement)
  if m:
    submit_folder = java_props[PROP_SUBMIT_APP_DIR]
    command = 'bash %s/submit %s -j %d &' % (submit_folder, m.group(2), job_id)
    FNULL = open(os.devnull, 'w')
    return subprocess.call(command, stdout=FNULL, stderr=subprocess.STDOUT, shell=True)
  else:
    print "'%s' is not a valid submit statement" % PROP_BROKER_URL
    sys.exit(-1)

def schedule_jobs(input_file, a_model, a_comment):
  global mysql_conn

  dir,file_name = os.path.split(input_file)
  subpath = os.path.basename(dir)
  model = subpath if a_model is None else a_model
  comment = file_name if a_comment is None else a_comment

  with open(input_file, 'r', 0x4000) as f:
    mysql_conn.start_transaction(isolation_level='READ UNCOMMITTED')
    cursor = mysql_conn.cursor(dictionary=True, buffered=True)
    cursor.execute("SELECT `AUTO_INCREMENT` FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'danube' AND TABLE_NAME = 'submit_batches'")
    result = cursor.fetchone()
    batch_id = int(result['AUTO_INCREMENT'])
    update = "INSERT INTO submit_batches (model, comments, queuing_ts) VALUES ('%s', '%s', SYSDATE())" % (model, comment)
    # print update
    cursor.execute(update)
    for line in f:
      if line[0] in ['#', '!']:
        continue
      line = line.rstrip()
      if line == '':
        continue
      m = re.match('(.*)(submit.*)( &)*', line)
      if m:
        statement = m.group(2)
        resource = extract_resource(statement)
        if resource:
          update = "INSERT INTO submit_jobs (batch_id, resource_type, submit_statement) VALUES (%d, '%s', '%s')" % (batch_id, resource, statement)
          # print update
          cursor.execute(update)
        else:
          print "ERROR: '%s' does not include a resource" % statement
          sys.exit(-1)
      else:
        print "ERROR: '%s' is not a valid statement" % line
        sys.exit(-1)

    mysql_conn.commit()
    cursor.close()
    print "Batch id:%d was successfull scheduled for model '%s' with comments: '%s'" % (batch_id, model, comment)


def extract_resource(statement):
  tokens = statement.split(' ')
  for i in range(len(tokens)):
    if tokens[i] == '-r' and (i+1) < len(tokens):
      return tokens[i+1]


def override_order(job_id, override):
  global mysql_conn

  cursor = mysql_conn.cursor(dictionary=True, buffered=True)
  if override:
    update = "UPDATE submit_jobs SET override = 1 WHERE id = %d" % (job_id)
  else:
    update = "UPDATE submit_jobs SET override = 0 WHERE id = %d" % (job_id)

  cursor.execute(update)
  mysql_conn.commit()
  cursor.close()
  if override:
    print "Job id:%d was successfull overrided with boosted order" % (job_id)
  else:
    print "Job id:%d was successfull reset with sequential order" % (job_id)

def error_handling(msg):
  print msg
  sys.exit(-1)

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Submit scheduling')
  parser.add_argument('-a', '--properties-app', help='Path of the application properties file (default is ' + APP_PROPERTIES + ')')
  parser.add_argument('-e', '--properties-env', help='Path of the environment-specific properties file (default is ' + ENV_PROPERTIES + ')')
  parser.add_argument('-f', '--jobs-input-file', help='Path of input file of submit jobs in batch.  It should work with queuing option')
  parser.add_argument('-m', '--model', help='The model number that the submit batch is against, (optional), take from subdirectory')
  parser.add_argument('-c', '--batch-comment', help=' comments for batch jobs for documentation purpose, ex model 2.4 release, (optional), take from filename')
  parser.add_argument('-j', '--job-id', help=' comments for batch jobs for documentation purpose, ex model 2.4 release, (optional), take from filename')
  parser.add_argument('-trigger', action='store_true')
  parser.add_argument('-schedule', action='store_true')
  parser.add_argument('-override', action='store_true')
  parser.add_argument('-reset', action='store_true')


  args = parser.parse_args()
  # print args
  app_properties = args.properties_app or APP_PROPERTIES
  env_properties = args.properties_env or ENV_PROPERTIES
  java_props = parse_validate_java_properties([app_properties, env_properties])
  mysql_conn = get_mysql_connection()

  if args.trigger:
      job_id, sub_command = get_next_submit_job()
      # print job_id, sub_command
      if sub_command:
        is_broker_ok = broker_available()
        is_mq_ok = mq_available()
        is_host_ok = host_available()
        # print is_broker_ok, is_mq_ok, is_host_ok
        if is_broker_ok and is_mq_ok and is_host_ok:
          status = submit_next_job(sub_command, job_id)
          if status == 0:
             print "'%s' was successfully triggered" % (sub_command)
  elif args.schedule:
    if args.jobs_input_file is None:
      print "ERROR: input file is required for scheduling submit jobs"
      sys.exit(-1)
    if not os.path.isfile(args.jobs_input_file):
      print "ERROR: input file '%s' was not found" % args.jobs_input_file
      sys.exit(-1)
    input_file = args.jobs_input_file
    schedule_jobs(input_file, args.model, args.batch_comment)
  elif args.override or args.reset:
    if args.override and args.reset:
      print "ERROR: You cannot have override/ reset in both ways"
      sys.exit(-1)
    if args.job_id is None:
      print "ERROR: -j job-id is required for override/ reset"
      sys.exit(-1)
    override_order(int(args.job_id), args.override)

  if mysql_conn:
    mysql_conn.close()







