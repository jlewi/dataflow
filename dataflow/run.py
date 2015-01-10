#!/usr/bin/python
#
# Script for running the examples.
#
# Requires python-gflags

import datetime
import gflags
import getpass
import os
import subprocess
import sys

gflags.DEFINE_string("command", "Run", "Command to run")
gflags.DEFINE_string("project", "dataflow-jlewi", "Which project to use")
FLAGS = gflags.FLAGS

now = datetime.datetime.now().strftime("%m%d-%H%M")
num_workers = 1

bucket = "gs://dataflow-jlewi"
input_path = "{bucket}/wordcount/input/nytimes-index.html".format(bucket=bucket)
output_path ="{bucket}/wordcout/output/counts".format(bucket=bucket)
staging_path = "{bucket}/wordcount/staging".format(bucket=bucket)

flags = {
  "runner": "BlockingDataflowPipelineRunner",
  "project": FLAGS.project, 
  "input": input_path, 
  "output": output_path, 
  "stagingLocation": staging_path, 
  "numWorkers": num_workers,
  "jobName": "app-{0}-{1}".format(getpass.getuser(), now)
}

main_class = "dataflow.App"

def RunWithoutMaven():
  command_line = ["java", "-cp", "./target/dataflow-1.0-SNAPSHOT.jar",
                  main_class]

  for k, v in flags.iteritems():
    command_line.append("--{0}={1}".format(k, v))
  
  subprocess.check_call(command_line)
  
def Run():
  command_line = ["mvn", "exec:java", "-Dexec.mainClass=" + main_class]
  java_args = []
  for k, v in flags.iteritems():
    java_args.append("--{0}={1}".format(k, v))
  command_line.append("-Dexec.args=" + " ".join(java_args))
  subprocess.check_call(command_line)

def Main(argv):  
  try:
    unparsed = FLAGS(argv)  # parse flags
  except gflags.FlagsError, e:
    usage = """Usage:
{name} <command> {flags}

where command is one of: {commands}
"""
    print "%s" % e
    print usage.format(name=argv[0], flags=FLAGS, commands=commands)
    sys.exit(1)

  command = FLAGS.command.strip().lower()
  if command == "run":
    Run()
  else:
    print "Unrecognized command: %s" % FLAGS.command
    sys.exit(1)


if __name__ == "__main__":
  Main(sys.argv)

