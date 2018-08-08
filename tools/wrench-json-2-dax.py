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
import json
import logging
import os
import xml.etree.cElementTree as ET
import xml.dom.minidom as MD

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


def _convert_to_dax3_xml(data):
    """
    Convert a JSON object to a DAX3 XML format
    :param data: JSON object
    :return: DAX3 XML object
    """
    root = ET.Element("adag")

    for job in data['workflow']['jobs']:
        if job['type'] == 'compute':
            j = job['name'].split('_')
            job_id = j[len(j) - 1]
            name = job['name'].split('_')[0]
            runtime = str(job['runtime'])
            job_element = ET.SubElement(root, 'job', id=job_id, namespace="Montage", name=name, version='1.0',
                                        runtime=runtime)

            for file in job['files']:
                size = str(file['size'])
                ET.SubElement(job_element, 'uses', file=file['name'], link=file['link'], register='true',
                              transfer='true', optional='false', type='data', size=size)

            if len(job['parents']) > 0:
                child = ET.SubElement(root, 'child', ref=job_id)
                for parent in job['parents']:
                    if not parent.startswith(('stage_', 'create_dir', 'cleanup', 'clean_up', 'register_')):
                        p = parent.split('_')
                        parent_id = p[len(p) - 1]
                        ET.SubElement(child, 'parent', ref=parent_id)

    return ET.ElementTree(root)

def main():
    # Application's arguments
    parser = argparse.ArgumentParser(description='Converts a JSON workflow trace to Pegasus DAX3 XML format.')
    parser.add_argument('json_wf', metavar='JSON_WORKFLOW', help='JSON workflow trace')
    parser.add_argument('-o', dest='output', action='store', help='Output filename')
    parser.add_argument('-d', '--debug', action='store_true', help='Print debug messages to stderr')
    args = parser.parse_args()

    # Configure logging
    _configure_logging(args.debug)

    # Sanity check
    if not os.path.isfile(args.json_wf):
        logger.error('The provided path does not exist or is not a file:\n\t' + args.json_wf)
        exit(1)

    # Read JSON file
    logger.info('Reading JSON file: ' + args.json_wf)
    with open(args.json_wf) as f:
        data = json.load(f)

    dax3_xml = _convert_to_dax3_xml(data)
    txt = ET.tostring(dax3_xml.getroot(), encoding="UTF-8", method="xml")

    if args.output:
        with open(args.output, 'w') as outfile:
            outfile.write(MD.parseString(txt).toprettyxml())
            logger.info('DAX3 XML file written to "%s".' % args.output)
    else:
        print(MD.parseString(txt).toprettyxml())

if __name__ == '__main__':
    main()
