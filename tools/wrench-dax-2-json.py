import collections
import datetime
import fnmatch
import logging
import os
import sys
import xml.etree.ElementTree

from wfcommons.common import File, FileLink, Task, TaskType, Workflow

logger = logging.getLogger(__name__)

for root, dirnames, filenames in os.walk(sys.argv[1]):
    for filename in fnmatch.filter(filenames, '*.xml'):
        dax_file = os.path.join(root, filename)

        logger.info('DAX file: ' + os.path.basename(dax_file))

        workflow = Workflow('os.path.basename(dax_file)')

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
            tasks_map = {}
            for j in e.findall('{http://pegasus.isi.edu/schema/DAX}job'):

                files = []

                for f in j.findall('{http://pegasus.isi.edu/schema/DAX}uses'):
                    file = File(
                        name=str(f.get('file')),
                        link=FileLink.OUTPUT if str(f.get('link')) == 'output' else FileLink.INPUT,
                        size=f.get('size')
                        )
                    files.append(file)

                task = Task(
                    name=str(j.get('name')).replace('_chr21', '') + '_' + str(j.get('id')),
                    task_type=TaskType.COMPUTE,
                    runtime=str(j.get('runtime')),
                    machine=None,
                    args=[],
                    cores=1,
                    avg_cpu=None,
                    bytes_read=None,
                    bytes_written=None,
                    memory=None,
                    energy=None,
                    avg_power=None,
                    priority=None,
                    files=files
                )
                tasks_map[str(j.get('id'))] = task
                workflow.add_node(task.name, task=task)

            for c in e.findall('{http://pegasus.isi.edu/schema/DAX}child'):
                child = str(c.get('ref'))
                for p in c.findall('{http://pegasus.isi.edu/schema/DAX}parent'):
                    parent = str(p.get('ref'))
                    workflow.add_edge(tasks_map[parent].name, tasks_map[child].name)

        except xml.etree.ElementTree.ParseError as ex:
            logger.warning(str(ex))

        # cleaning temporary file
        if temp_file:
            os.remove(dax_file)

        workflow.write_json(dax_file.replace('.xml', '.json'))
