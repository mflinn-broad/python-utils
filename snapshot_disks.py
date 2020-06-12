#!/usr/bin/env python

import time
import argparse
from pprint import pprint
from datetime import date

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials


def get_args():
    parser = argparse.ArgumentParser(description="Disk snapshotter")
    parser.add_argument('--project', '-p', required=True, help='GCP project id')
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

def build_compute_client():
    credentials = GoogleCredentials.get_application_default()
    return discovery.build('compute', 'v1', credentials=credentials)
    
def get_disks(client, project, zone):
    disk_list = []
    request = client.disks().list(project=project, zone=zone)
    print(f'fetching disks in project: {project}')
    while request is not None:
        response = request.execute()

        for disk in response['items']:
            disk_name = disk['name']
            if not disk_name.startswith('gke-') and 'tester' not in disk_name:
                disk_list.append(disk_name)
        request = client.disks().list_next(previous_request=request, previous_response=response)
    
    return disk_list

def snapshot_disks(client, project, zone, disk_list):
    current_date = date.today()
    for disk in disk_list:
        print(f'Snapshotting {disk}...')
        snapshot_body = {
            'name': f'{disk}-{current_date}'
        }
        request = client.disks().createSnapshot(project=project, zone=zone, disk=disk, body=snapshot_body)
        response = request.execute()
        wait_for_operation(client, project, zone, response['name'])
        print(f'Snaphot of disk: {disk} complete')

    print('Snapshots complete!')

def main():
    compute = build_compute_client()
    project_id = get_args()
    zone = 'us-central1-a'
    disk_list = get_disks(compute, project_id, zone)
    snapshot_disks(compute, project_id, zone, disk_list)

if __name__ == '__main__':
    main()
    

