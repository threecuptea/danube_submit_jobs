### danube_submit_job is an in-house application tuned to the needs of Danube pipeline.  We have to manually pace submitting lots of resources when we push Danube transformation changes since there is limited infrastsructure resource.  Danube_sumbit_job automates the process. The application allows the user to submit a batch of multiple jobs once then the cron job pick in to submit the individual job if all required conditions are met.   The required conditions, including the maximum message count in mysql queue table, the maximum memory used by RabbitMQ and the minimum free memory of the server hosting the submission application, are configurable. 
#### submit_job.py is a rich python application.  I use it as a reference application.  It includes the following techinques:
     
1. subprocess to open a sub-process to execute bash or other application.  It has 'call' method and 'Popen' 
   (pipe open method).  The followings are two examples:

    
        command = 'bash %s/submit %s -j %d &' % (submit_folder, m.group(2), job_id)    
        FNULL = open(os.devnull, 'w')
        return subprocess.call(command, stdout=FNULL, stderr=subprocess.STDOUT, shell=True) 
     
        describe_cluster = 'aws emr describe-cluster --cluster-id %s' % cluster_id    
        proc_describe_cluster = subprocess.Popen(describe_cluster.split(), stdout=subprocess.PIPE)    
        stdoutdata, _ = proc_describe_cluster.communicate()     
       
2.  json has popular dump (print pretty), load (from io) and loads (from string) methods. 
    It is in dictionary form once parsed.

   
        print json.dumps({'4': 5, '6': 7}, sort_keys=True, indent=4, separators=(',', ': '))    
        cluster_id = json.load(sys.stdin)['ClusterId']
     
        stdoutdata, _ = proc_describe_cluster.communicate()    
        describe_cluster_json = json.loads(stdoutdata)    
        state = describe_cluster_json['Cluster']['Status']['State']
    
    
 3. re (regular expression) has match method   
    
    
        m = re.match('jdbc:mysql://(.*)/(.*)', java_props[PROP_BROKER_URL])
     
        if m:
          host = m.group(1)
          database = m.group(2)
      
      
 4. mysql.connector     
 
 
        mysql_conn = mysql.connector.connect(user=user_name, password=password, host=host, database=database)     
        cursor = mysql_conn.cursor(dictionary=True, buffered=True)     
        cursor.execute("SELECT `AUTO_INCREMENT` FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'danube' AND TABLE_NAME = 'submit_batches'")
        result = cursor.fetchone()     
        batch_id = int(result['AUTO_INCREMENT'])     
        update = "INSERT INTO submit_batches (model, comments, queuing_ts) VALUES ('%s', '%s', SYSDATE())" % (model, comment)
        cursor.execute(update)
     
 5. httplib
     
     
        auth = 'Basic ' + base64.b64encode(username + ':' + password)     
        conn = httplib.HTTPConnection(host, DEFAULT_MQ_ADMIN_PORT, True, 30)     
        conn.connect()
     
        conn.request('GET', '/api/nodes', headers={'authorization': auth})
        resp = conn.getresponse()     
        if resp.status == 200:
          node = json.loads(resp.read())[0]     
     
 6. argparse
     
     
        parser = argparse.ArgumentParser(description='Submit scheduling')
        parser.add_argument('-f', '--jobs-input-file', help='Path of input file of submit jobs in batch.  It should work with queuing option')
        parser.add_argument('-trigger', action='store_true')
        parser.add_argument('-schedule', action='store_true')
      
        args = parser.parse_args()
        if args.trigger:
          :
        elif args.schedule:
          if args.jobs_input_file is None:  
        
 7. os.path has isFile, dirname, basename, join methods.  raw_input is a built-in method.
        
     
        if os.path.isfile(path):
          with open(path, 'r', 0x4000) as f:
            for line in f:
              if line[0] in ['#', '!']:
                continue
              line = line.rstrip()
            
        dir,file_name = os.path.split(input_file)
        subpath = os.path.basename(dir)    
        
        parentDir = dirname(dirname(__file__))
        ratingsFile = join(parentDir, "data/personalRatings.txt")
        
        r = raw_input("Looks like you've already rated the movies. Overwrite ratings (y/N)? ") 
