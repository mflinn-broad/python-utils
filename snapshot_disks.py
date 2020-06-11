#!/usr/bin/env python

from pprint import pprint
import argparse
from datetime import date
import time

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials


def get_args():
    parser = argparse.ArgumentParser(description="Disk snapshotter")
    parser.add_argument('--project', '-p',required=True, help='GCP project id')
    args = parser.parse_args()
    return args.project

def wait_for_operation(compute, project, zone, operation):
    print('Waiting for operation to finish...')
    while True:
        result = compute.zoneOperations().get(
            project=project,
            zone=zone,
            operation=operation).execute()

        if result['status'] == 'DONE':
            print("done.")
            if 'error' in result:
                raise Exception(result['error'])
            return result

        time.sleep(1)

if __name__ == '__main__':
    disk_list = []
    credentials = GoogleCredentials.get_application_default()
    service = discovery.build('compute', 'v1', credentials=credentials)
    project_id = get_args()
    zone = 'us-central1-a'
    request = service.disks().list(project=project_id, zone=zone)
    print(f'fetching disks in project: {project_id}')
    while request is not None:
        response = request.execute()

        for disk in response['items']:
            disk_name = disk['name']
            if not disk_name.startswith('gke-') and 'tester' not in disk_name:
                disk_list.append(disk_name)
        request = service.disks().list_next(previous_request=request, previous_response=response)
    
    print('Found disks:')
    print(disk_list)

    current_date = date.today()
    for disk in disk_list:
        print(f'Snapshotting {disk}...')
        snapshot_body = {
            'name': f'{disk}-{current_date}'
        }
        request = service.disks().createSnapshot(project=project_id, zone=zone, disk=disk, body=snapshot_body)
        response = request.execute()
        wait_for_operation(service, project_id, zone, response['name'])
        print(f'Snaphot of disk: {disk} complete')

    print('Snapshots complete!')

