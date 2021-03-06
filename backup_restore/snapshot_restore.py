#!/usr/bin/env python

import argparse
import datetime
import sys
import time
from pprint import pprint

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

def get_args():
    parser = argparse.ArgumentParser(description="snapshot restore arguments")
    parser.add_argument('--project', required=True, help="GCP project of target host")
    parser.add_argument('--instance', required=True, help="Name of target host to restore")
    args = parser.parse_args()
    return args

def wait_for_operation(compute, project, zone, operation):
    while True:
        result = compute.zoneOperations().get(
            project=project,
            zone=zone,
            operation=operation).execute()

        if result['status'] == 'DONE':
            sys.stdout.write('\n')
            if 'error' in result:
                raise Exception(result['error'])
            return result
        time.sleep(1)

def build_compute_client():
    credentials = GoogleCredentials.get_application_default()
    return discovery.build('compute', 'v1', credentials=credentials)

def get_disks_from_instance(client, instance_name, project):
    instance_disks = []
    zone = 'us-central1-a'
    request = client.instances().get(project=project, zone=zone, instance=instance_name)
    response = request.execute()
    log(f'retrieving disks for instance: {instance_name}')
    for disk in response['disks']:
        disk_name = (disk['source'].rsplit('/', 1))[-1]
        device_name = disk['deviceName']
        instance_disks.append((disk_name, device_name))
    log(f'found disks: {instance_disks}')
    return instance_disks

def get_snapshots_by_disk(client, disk_name, project):
    log(f'retrieving most recent snapshot of disk: {disk_name}')
    full_disk_name = f'https://www.googleapis.com/compute/v1/projects/{project}/zones/us-central1-a/disks/{disk_name}'
    disk_snapshots = []
    request = client.snapshots().list(
        project=project,
    )
    while request is not None:
        response = request.execute()
        for snapshot in response['items']:
            # Return most recent snapshort of desired disk
            if (snapshot['sourceDisk'] == full_disk_name):
                log(f"found snapshot {snapshot['name']}")
                disk_snapshots.append((snapshot['name'], snapshot['creationTimestamp']))
        request = client.snapshots().list_next(previous_request=request, previous_response=response)
    disk_snapshots.sort(key=lambda x: x[1], reverse=True)
    return disk_snapshots[0][0]

def get_instance_snapshots(client, instance, project):
    current_disks = get_disks_from_instance(client, instance, project)
    disk_snapshots = []
    for disk, device in current_disks:
        disk_snapshots.append((get_snapshots_by_disk(client, disk, project), disk == instance))
    log(f'found snapshots: {[snapshot[0] for snapshot in disk_snapshots]} for instance {instance}')
    return disk_snapshots

def build_disk_from_snap(client, snapshot, is_boot, project):
    disk_name = (f'{snapshot}-restore-{datetime.date.today()}')[:62] # Truncate to 62 chars for google api limits
    # when truncating last char must be alpha-numeric
    if disk_name[-1] == '-':
        disk_name = disk_name[:-1]
    req_body = {
        'name': disk_name, 
        'sourceSnapshot': f'https://www.googleapis.com/compute/v1/projects/{project}/global/snapshots/{snapshot}',
        'type': f'https://www.googleapis.com/compute/v1/projects/{project}/zones/us-central1-a/diskTypes/{"pd-standard" if is_boot else "pd-ssd"}' 
    }
    log(f'Building restore disk from snapshot: {snapshot}...')
    request = client.disks().insert(project=project, zone='us-central1-a', body=req_body)
    response = request.execute()
    wait_for_operation(client, project, 'us-central1-a', response['name'])
    log(f'Succesfully built disk: {disk_name} from snapshot: {snapshot}')
    return disk_name

def instance_stop(client, instance, project):
    request = client.instances().stop(project=project, zone='us-central1-a', instance=instance)
    log(f'stopping instance: {instance}')
    response = request.execute()
    wait_for_operation(client, project, 'us-central1-a', response['name'])
    log(f'instance {instance} stopped.')

def instance_start(client, instance, project):
    request = client.instances().start(project=project, zone='us-central1-a', instance=instance)
    log(f'starting instance: {instance}')
    response = request.execute()
    wait_for_operation(client, project, 'us-central1-a', response['name'])
    log(f'instance {instance} started.')

def detach_disks(client, instance, project):
    disks = get_disks_from_instance(client, instance, project)
    for disk, device in disks:
        detach_disk(client, instance, disk, device, project)

def detach_disk(client, instance, disk, device, project):
    log(f'detaching disk: {disk} from instance: {instance}')
    request = client.instances().detachDisk(project=project, zone='us-central1-a', instance=instance, deviceName=device)
    response = request.execute()
    wait_for_operation(client, project, 'us-central1-a', response['name'])
    log(f' Successfully detached {disk} from {instance}')

def attach_disk(client, instance, disk, is_boot, project):
    log(f'attaching disk: {disk} to instance {instance}')
    req_body = {
        'source': f'https://www.googleapis.com/compute/v1/projects/{project}/zones/us-central1-a/disks/{disk}',
        'boot': is_boot
    }
    request = client.instances().attachDisk(project=project, zone='us-central1-a', instance=instance, body = req_body)
    response = request.execute()
    wait_for_operation(client, project, 'us-central1-a', response['name'])
    log(f'successfully attached disk {disk} to instance {instance}')

def attach_disks(client, instance, disk_list, project):
    for disk, is_boot in disk_list:
        attach_disk(client, instance, disk, is_boot, project)

# Google oauth library seems to have a conflict with logging module, quick workaround
def log(message):
    print(f"{datetime.datetime.now()} [INFO] {message}")

def main():
    args = get_args()
    log("Connecting to Google Cloud...")
    compute = build_compute_client()
    snapshots = get_instance_snapshots(compute, args.instance, args.project)  
    restored_disks = []
    for snapshot, is_boot in snapshots:
        restored_disks.append((build_disk_from_snap(compute, snapshot, is_boot, args.project), is_boot))
    instance_stop(compute, args.instance, args.project)
    detach_disks(compute, args.instance, args.project)
    attach_disks(compute, args.instance, restored_disks, args.project)
    instance_start(compute, args.instance, args.project)

if __name__ == '__main__':
    main()