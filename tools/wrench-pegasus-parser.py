#!/usr/bin/env python
#
# Copyright (c) 2018-2020. The WRENCH Team.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#

import argparse
import collections
import datetime
import getpass
import json
import logging
import os
import xml.etree.ElementTree
import fnmatch

__author__ = "Rafael Ferreira da Silva"

SCHEMA_VERSION = '1.0'

logger = logging.getLogger(__name__)
files_map = {}


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


def _get_trace_base(args):
    """
    Generate the base structure of the JSON trace file.
    :param args: list of arguments from argsparse
    :return: the base structure for the JSON trace
    """
    if args.author_email:
        email = args.author_email
    else:
        logger.warning('Please, updated the email address, or use the "-e" option.')
        email = 'support@wrench-project.org'

    if args.ignore_auxiliary:
        logger.warning('Ignoring Pegasus auxiliary jobs.')

    # parsing braindump.txt
    braindump_file = args.pegasus_dir + '/braindump.txt'
    version = None
    trace_name = None
    timestamp = None

    if not os.path.exists(braindump_file):
        logger.error('Unable to find braindump.txt file in:\n\t' + args.pegasus_dir)

    else:
        with open(braindump_file) as f:
            for line in f:
                if line.startswith('planner_version'):
                    version = line.split()[1]

                elif line.startswith('pegasus_wf_name'):
                    trace_name = line.split()[1]

                elif line.startswith('timestamp'):
                    timestamp = line.split()[1]

    if not version:
        logger.warning('Unable to determine pegasus version.')
        version = 'Undefined'
    if not trace_name:
        logger.warning(
            'Unable to determine trace name from "pegasus_wf_name" (braindump.txt). Name needs to be updated.')
        trace_name = 'Undefined'
    if not timestamp:
        logger.warning(
            'Unable to determine execution timestamp from "timestamp" (braindump.txt). Timestamp needs to be updated.')
        timestamp = 'Undefined'

    logger.debug('Creating JSON trace based on schema version %s' % SCHEMA_VERSION)
    data = collections.OrderedDict()
    data['name'] = trace_name
    if args.description:
        data['description'] = args.description
    else:
        data['description'] = 'Trace generated with wrench-pegasus-parser.py from http://wrench-project.org'
    data['createdAt'] = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    data['schemaVersion'] = SCHEMA_VERSION
    data['wms'] = {
        'name': 'Pegasus',
        'version': version,
        'url': 'http://pegasus.isi.edu'
    }
    data['author'] = {
        'name': args.author_name,
        'email': email
    }
    data['workflow'] = {
        'executedAt': timestamp,
        'machines': [],  # List of machines on which jobs executed
        'jobs': []
    }
    return data


def _parse_dax(workflow, pegasus_dir):
    """
    Parse the DAX file
    :param workflow: workflow object
    :param pegasus_dir: pegasus workflow submit dir
    """
    dax_list = _fetch_all_files(pegasus_dir, "dax", "")
    if len(dax_list) < 1:
        dax_list = _fetch_all_files(pegasus_dir, "xml", "")
        if len(dax_list) < 1:
            logger.error('The directory contains no ".dax" or ".xml" file')
            exit(1)

    dax_file = dax_list[0]
    logger.info('DAX file: ' + os.path.basename(dax_file))
    logger.debug('Processing DAX file.')

    jobs = {}

    line_num = 0
    temp_file = None
    with open(dax_file) as f:
        for line in f:
            line_num += 1
            if line.startswith('<?xml'):
                if line_num == 1:
                    break
                else:
                    temp_file = open('.pegasus-parser-tmp', 'w')

            if line.startswith('</adag>'):
                temp_file.write(line)
                dax_file = temp_file.name
                temp_file.close()
                break

            if temp_file:
                temp_file.write(line)

    try:
        e = xml.etree.ElementTree.parse(dax_file).getroot()
        for j in e.findall('{http://pegasus.isi.edu/schema/DAX}job'):
            job = collections.OrderedDict()
            job['name'] = str(j.get('name')) + '_' + str(j.get('id'))
            job['type'] = 'compute'
            job['runtime'] = 0
            job['parents'] = []
            job['files'] = []

            for f in j.findall('{http://pegasus.isi.edu/schema/DAX}uses'):
                file = collections.OrderedDict()
                file['link'] = f.get('link')
                file['name'] = f.get('name') if not f.get('name') is None else f.get('file')
                file['size'] = 0
                job['files'].append(file)

            workflow['jobs'].append(job)

    except xml.etree.ElementTree.ParseError as ex:
        logger.warning(str(ex))

    # cleaning temporary file
    if temp_file:
        os.remove(dax_file)


def _parse_dag(workflow, pegasus_dir, ignore_auxiliary):
    """
    Parse the DAG file
    :param workflow: workflow object
    :param pegasus_dir: pegasus workflow submit dir
    :param ignore_auxiliary: ignore auxiliary jobs
    """
    dags_list = _fetch_all_files(pegasus_dir, "dag", "")
    if len(dags_list) < 1:
        logger.error('The directory contains no ".dag" file')
        exit(1)

    dag_file = dags_list[0]
    logger.info('DAG file: ' + os.path.basename(dag_file))
    logger.debug('Processing DAG file.')

    num_jobs = 0
    jobs_set = set()
    with open(dag_file) as f:
        for line in f:
            if line.startswith('JOB'):
                num_jobs += 1
                job_name = line.split()[1]

                # find the job
                job = None
                for j in workflow['jobs']:
                    if j['name'].lower() == job_name.lower():
                        job = j
                        break

                if not job and not ignore_auxiliary:
                    job = collections.OrderedDict()
                    job['name'] = job_name
                    job['type'] = 'compute'
                    job['runtime'] = 0
                    job['parents'] = []
                    job['files'] = []
                    workflow['jobs'].append(job)
                else:
                    _parse_meta_file({'name': job_name}, pegasus_dir)

                # Parsing job stdout file
                if job:
                    jobs_set.add(job['name'])
                    _parse_job_output(workflow, job, pegasus_dir)

            elif line.startswith('PARENT'):
                # Typically, parent/child references are at the end of the DAG file
                s = line.split()
                parent = s[1]
                child = s[3]
                for j in workflow['jobs']:
                    if j['name'] == child and parent in jobs_set:
                        j['parents'].append(parent)
                        break

    # parse workflow files
    for j in workflow['jobs']:
        if 'files' in j:
            for f in j['files']:
                if f['name'] in files_map:
                    f['size'] = int(files_map[f['name']])

    # parse workflow makespan
    dagman_file = dag_file + '.dagman.out'
    logger.debug('Processing DAGMAN output file.')
    with open(dagman_file) as f:
        lines = f.readlines()
        try:
            s = datetime.datetime.strptime(' '.join(lines[0].split()[0:2]), '%m/%d %H:%M:%S')
            e = datetime.datetime.strptime(' '.join(lines[-1].split()[0:2]), '%m/%d %H:%M:%S')
        except ValueError:
            try:
                s = datetime.datetime.strptime(' '.join(lines[0].split()[0:2]), '%m/%d/%y %H:%M:%S')
                e = datetime.datetime.strptime(' '.join(lines[-1].split()[0:2]), '%m/%d/%y %H:%M:%S')
            except ValueError as error:
                logger.error(error)
                exit(1)

        workflow['makespan'] = (e - s).total_seconds()

    logger.debug('Found %s jobs.' % num_jobs)


def _parse_job_output(workflow, job, pegasus_dir):
    """
    Parse the kickstart job output file (e.g., .out.000).
    :param workflow: workflow object
    :param job: job object
    :param pegasus_dir: pegasus workflow submit dir
    """
    output_list = _fetch_all_files(pegasus_dir, "out.*", job['name'])
    if len(output_list) == 0:
        logger.warning('Job has no kickstart record. Skipping it.')
        if job['name'].startswith(('stage_', 'create_dir', 'cleanup', 'clean_up', 'register_')):
            job['type'] = 'auxiliary'
        return

    if len(output_list) > 1:
        logger.debug('Job "%s" has multiple runs. Parsing last attempt.' % job['name'])
    output_file = output_list[-1]

    runtime = 0
    total_time = 0
    bytes_read = 0
    bytes_written = 0
    memory = 0
    args = []
    files = []
    machine = {}
    machines = workflow['machines']

    # clean output file from PBS logs
    line_num = 0
    temp_file = None
    with open(output_file) as f:
        for line in f:
            line_num += 1
            if line.startswith('<?xml'):
                if line_num == 1:
                    break
                else:
                    temp_file = open('.pegasus-parser-tmp', 'w')

            if line.startswith('</invocation>'):
                temp_file.write(line)
                output_file = temp_file.name
                temp_file.close()
                break

            if temp_file:
                temp_file.write(line)

    try:
        e = xml.etree.ElementTree.parse(output_file).getroot()
        # main job information
        if e.get('transformation').startswith('pegasus:') or job['name'].startswith('chmod_'):
            job['type'] = 'auxiliary'

        if job['name'].startswith(('stage_in_', 'stage_out')):
            job['type'] = 'transfer'

        for mj in e.findall('{http://pegasus.isi.edu/schema/invocation}mainjob'):
            runtime += float(mj.get('duration'))

            # get average cpu utilization
            for u in mj.findall('{http://pegasus.isi.edu/schema/invocation}usage'):
                total_time += float(u.get('utime')) + float(u.get('stime'))

            # get job arguments
            for av in mj.findall('{http://pegasus.isi.edu/schema/invocation}argument-vector'):
                for a in av.findall('{http://pegasus.isi.edu/schema/invocation}arg'):
                    args.append(a.text)

            # get job memory information
            for p in mj.findall('{http://pegasus.isi.edu/schema/invocation}proc'):
                memory += float(p.get('rsspeak'))
                bytes_read += max(int(p.get('rbytes')), int(p.get('rchar')))
                bytes_written += max(int(p.get('wbytes')), int(p.get('wchar')))

        # machine
        for m in e.findall('{http://pegasus.isi.edu/schema/invocation}machine'):
            for u in m.findall('{http://pegasus.isi.edu/schema/invocation}uname'):
                machine['system'] = u.get('system')
                machine['architecture'] = u.get('machine')
                machine['release'] = u.get('release')
                machine['nodeName'] = u.get('nodename')
            for u in m.findall('{http://pegasus.isi.edu/schema/invocation}linux'):
                for r in u.findall('{http://pegasus.isi.edu/schema/invocation}ram'):
                    machine['memory'] = int(r.get('total'))
                for c in u.findall('{http://pegasus.isi.edu/schema/invocation}cpu'):
                    machine['cpu'] = {
                        'count': int(c.get('count')),
                        'speed': int(c.get('speed')),
                        'vendor': c.get('vendor')
                    }
            # Append machine to the list of machines
            # Check if machine is already present in the list
            has_machine = False
            for m in machines:
                if m['nodeName'] == machine['nodeName']:
                    has_machine = True
                    break
            if not has_machine:
                machines.append(machine)

        job['runtime'] = runtime
        if total_time > 0:
            job['avgCPU'] = float('%.4f' % (100 * (total_time / runtime)))
        if memory > 0:
            job['memory'] = memory
        if bytes_read > 0:
            job['bytesRead'] = bytes_read
        if bytes_written > 0:
            job['bytesWritten'] = bytes_written
        if len(args) > 0:
            job['arguments'] = args
        if len(machine) > 0:
            # referring to the machine by it's nodeName
            job['machine'] = machine['nodeName']

    except xml.etree.ElementTree.ParseError as ex:
        # parse create_dir output file
        if job['name'].startswith(('stage_', 'create_dir')):
            job['type'] = 'auxiliary'
            with open(output_file) as f:
                s = None
                e = None
                for line in f:
                    values = line.split()
                    if not s:
                        s = datetime.datetime.strptime(values[0] + ' ' + values[1], '%Y-%m-%d %H:%M:%S,%f')
                    e = datetime.datetime.strptime(values[0] + ' ' + values[1], '%Y-%m-%d %H:%M:%S,%f')
                job['runtime'] = (e - s).total_seconds()

        else:
            logger.warning(job['name'] + ': ' + str(ex))

    # cleaning temporary file
    if temp_file:
        os.remove(output_file)

    # parsing meta file
    _parse_meta_file(job, pegasus_dir)

    # parsing .in file for stage in/out jobs
    # if job['name'].startswith(('stage_in_', 'stage_out_')):
    #     # job['runtime'] = 0
    #     in_list = _fetch_all_files(pegasus_dir, "in", job['name'])
    #     if not in_list:
    #         logger.warning('Job %s has no .in record. Skipping it.' % job['name'])
    #     else:
    #         link = 'input' if job['name'].startswith('stage_in_') else 'output'
    #         with open(in_list[0]) as inlist:
    #             m = json.load(inlist)
    #             for f in m:
    #                 name = f['lfn']
    #                 size = files_map[name] if name in files_map else 0
    #                 job['files'].append({
    #                     'link': link,
    #                     'name': name,
    #                     'size': size,
    #                     'src': f['src_urls'][0]['site_label'],
    #                     'dest': f['dest_urls'][0]['site_label']
    #                 })

    # parsing .sub file to get job priorities
    sub_list = _fetch_all_files(pegasus_dir, "sub", job['name'])
    if not sub_list:
        logger.warning('Job %s has no .sub record. Skipping it.' % job['name'])
    else:
        with open(sub_list[0]) as f:
            for line in f:
                if line.startswith('priority'):
                    job['priority'] = int(line.split()[2])


def _parse_meta_file(job, pegasus_dir):
    """
        Parse the Pegasus meta file (generated from pegasus-integrity)
        :param job: job object
        :param pegasus_dir: pegasus workflow submit dir
        """
    meta_list = _fetch_all_files(pegasus_dir, "meta", job['name'])
    if not meta_list:
        logger.warning('Job %s has no meta record (skipping meta analysis).' % job['name'])
    else:
        with open(meta_list[0]) as metadata:
            m = json.load(metadata)
            for f in m:
                if f['_id'] not in files_map:
                    files_map[f['_id']] = f['_attributes']['size']


def main():
    # Application's arguments
    parser = argparse.ArgumentParser(description='Parse Pegasus submit directory to generate trace file.')
    parser.add_argument('pegasus_dir', metavar='PEGASUS_DIR', help='Pegasus submit directory')
    parser.add_argument('-a', dest='author_name', action='store', help='Author\'s name', default=getpass.getuser())
    parser.add_argument('-e', dest='author_email', action='store', help='Author\'s email')
    parser.add_argument('-n', dest='description', action='store', help='Trace description')
    parser.add_argument('-o', dest='output', action='store', help='Output filename')
    parser.add_argument('-x', '--ignore-auxiliary', dest='ignore_auxiliary', action='store_true',
                        help='Ignore auxiliary jobs')
    parser.add_argument('-d', '--debug', action='store_true', help='Print debug messages to stderr')
    args = parser.parse_args()

    # Configure logging
    _configure_logging(args.debug)

    # Sanity check
    if not os.path.isdir(args.pegasus_dir):
        logger.error('The provided path does not exist or is not a directory:\n\t' + args.pegasus_dir)
        exit(1)

    # Generates Traces for JSON schema version defined in SCHEMA_VERSION
    trace = _get_trace_base(args)
    workflow = trace['workflow']

    # parse DAX file
    _parse_dax(workflow, args.pegasus_dir)

    # parse DAG file
    _parse_dag(workflow, args.pegasus_dir, args.ignore_auxiliary)

    if args.output:
        with open(args.output, 'w') as outfile:
            json.dump(trace, outfile, indent=2)
            logger.info('JSON trace file written to "%s".' % args.output)
    else:
        print(json.dumps(trace, indent=2))


if __name__ == '__main__':
    main()
