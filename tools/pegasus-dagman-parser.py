#!/usr/bin/env python
#
# Copyright (c) 2018. The WRENCH Team.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#

import argparse
import csv
import datetime
import json
import logging
import os
import sys
import fnmatch

__author__ = "Rafael Ferreira da Silva"

logger = logging.getLogger(__name__)


def _configure_logging(debug):
    """
    Configure the application's logging.
    :param debug: whether debugging is enabled
    """
    if debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)


def _fetch_all_files(input_dir, extension, file_name=""):
    """
    fetches all files from the directory and its hierarchy
    :param dir: input directory where files with extension should be searched
    :param extension: file extension to be searched for
    :param file_name: file_name to be searched
    :return:
    """
    files = []
    for root, dirnames, filenames in os.walk(input_dir):
        if len(file_name) == 0:
            for filename in fnmatch.filter(filenames, '*.' + extension):
                files.append(os.path.join(root, filename))
        else:
            for filename in fnmatch.filter(filenames, file_name + '.' + extension):
                files.append(os.path.join(root, filename))
    return files


def _parse_dagman_file(workflow, pegasus_dir):
    """
    Parse the DAGMan file
    :param workflow: workflow object
    :param pegasus_dir: pegasus workflow submit dir
    """
    files_list = _fetch_all_files(pegasus_dir, "dag.dagman.out", "")
    if len(files_list) < 1:
        logger.error('The directory contains no ".dag.dagman.out" file')
        exit(1)

    dagman_file = files_list[0]
    logger.info('DAGMan file: ' + os.path.basename(dagman_file))
    logger.debug('Processing DAGMan file.')

    start_time = 0

    with open(dagman_file) as f:
        for line in f:
            # event time
            try:
                event_time = datetime.datetime.strptime(' '.join(line.split()[0:2]), '%m/%d %H:%M:%S')
            except ValueError:
                try:
                    event_time = datetime.datetime.strptime(' '.join(line.split()[0:2]),
                                                            '%m/%d/%y %H:%M:%S')
                except ValueError as error:
                    logger.error(error)
                    exit(1)

            event_time = (event_time - datetime.datetime(1970, 1, 1)).total_seconds()

            if start_time == 0:
                start_time = event_time

            s = line.split()
            if 'Submitting HTCondor Node' in line:
                type = s[5][:s[5].find('_')]
                if type.startswith('clean'):
                    type = 'clean_up'
                elif type.startswith('create'):
                    type = 'create_dir'
                elif type.startswith('stage'):
                    type = s[5][:s[5].find('_', s[5].find('_') + 1)]
                workflow['tasks'].append({'id': s[5], 'start_time': event_time - start_time, 'type': type})

            if 'job completed' in line:
                for task in workflow['tasks']:
                    if task['id'] == s[3]:
                        task['end_time'] = event_time - start_time
                        task['walltime'] = task['end_time'] - task['start_time']
                        break


def _parse_json_file(workflow, workflow_json):
    """
    Parse the Worklfow JSON file
    :param workflow: workflow object
    :param workflow_json: pegasus workflow json file
    """
    with open(workflow_json) as workflow_file:
        w = json.load(workflow_file)
        for job in w['workflow']['jobs']:
            for j in workflow['tasks']:
                if job['name'] == j['id']:
                    j['duration'] = job['runtime']
                    j['level'] = 0
                    break


def main():
    # Application's arguments
    parser = argparse.ArgumentParser(
        description='Parse Pegasus submit directory to generate CSV file of tasks execution.')
    parser.add_argument('pegasus_dir', metavar='PEGASUS_DIR', help='Pegasus submit directory')
    parser.add_argument('workflow_json', metavar='WORKFLOW_JSON', help='JSON workflow generated from Pegasus')
    parser.add_argument('-o', dest='output', action='store', help='Output filename')
    parser.add_argument('-c', '--csv', action='store_true', help='CSV Format')
    parser.add_argument('-d', '--debug', action='store_true', help='Print debug messages to stderr')
    args = parser.parse_args()

    # Configure logging
    _configure_logging(args.debug)

    # Sanity check
    if not os.path.isdir(args.pegasus_dir):
        logger.error('The provided path does not exist or is not a directory:\n\t' + args.pegasus_dir)
        exit(1)

    workflow = {'tasks': []}

    # parse DAG file
    _parse_dagman_file(workflow, args.pegasus_dir)

    # parse JSON file
    _parse_json_file(workflow, args.workflow_json)

    if args.output:

        if not args.csv:
            with open(args.output, 'w') as outfile:
                json.dump(workflow, outfile, indent=2)
                logger.info('JSON trace file written to "%s".' % args.output)
        else:
            outfile = csv.writer(open(args.output, "wb+"))
            outfile.writerow(['engine', 'task', 'start', 'end', 'walltime', 'duration', 'level', 'type'])
            for task in workflow['tasks']:
                outfile.writerow(['pegasus',
                                  task['id'],
                                  task['start_time'],
                                  task['end_time'],
                                  task['walltime'],
                                  task['duration'],
                                  task['level'],
                                  task['type']
                                  ])

    else:
        if not args.csv:
            print(json.dumps(workflow, indent=2))
        else:
            out = csv.writer(sys.stdout)
            out.writerow(['engine', 'task', 'start', 'end', 'walltime', 'duration', 'level', 'type'])
            for task in workflow['tasks']:
                out.writerow(['pegasus',
                              task['id'],
                              task['start_time'],
                              task['end_time'],
                              task['walltime'],
                              task['duration'],
                              task['level'],
                              task['type']
                              ])


if __name__ == '__main__':
    main()
