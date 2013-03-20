# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2013 Rackspace
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from nova.api.openstack.compute.contrib import scheduled_images
from nova import db
from nova.tests.api.openstack import fakes
from qonos.qonosclient import client as qonos_client


def stub_out_instance(stubs, uuids):
    FAKE_INSTANCES = [
        fakes.stub_instance(1,
                            uuid=uuids[0],
                            auto_disk_config=False),
        fakes.stub_instance(2,
                            uuid=uuids[1],
                            auto_disk_config=True)
    ]

    def fake_instance_get_by_uuid(context, uuid):
        for instance in FAKE_INSTANCES:
            if uuid == instance['uuid']:
                return instance

    stubs.Set(db, 'instance_get_by_uuid',
                   fake_instance_get_by_uuid)


def stub_out_instance_system_metadata(stubs):
    def fake_instance_system_metadata_get(context, instance_id):
        return {'OS-SI:image_schedule': '{"retention": "6"}'}

    stubs.Set(db, 'instance_system_metadata_get',
            fake_instance_system_metadata_get)

    meta = {"OS-SI:image_schedule": '{"retention": "7"}'}

    def fake_instance_system_metadata_update(context, instance_id, meta,
                                             delete):
        return {'OS-SI:image_schedule': '{"retention": "7"}'}

    stubs.Set(db, 'instance_system_metadata_update',
            fake_instance_system_metadata_update)


def stub_out_qonos_client(stubs):
    def fake_qonos_client_list_schedules(cls, **kwargs):
        schedules = [{'id': 1}, {'id': 2}]
        return schedules

    stubs.Set(qonos_client.Client, 'list_schedules',
            fake_qonos_client_list_schedules)

    def fake_qonos_client_create_schedule(cls, schedule):
        return {}

    stubs.Set(qonos_client.Client, 'create_schedule',
            fake_qonos_client_create_schedule)

    def fake_qonos_client_delete_schedule(cls, schedules):
        return

    stubs.Set(qonos_client.Client, 'delete_schedule',
            fake_qonos_client_delete_schedule)

    def fake_qonos_client_update_schedule(cls, schedules, sch_body):
        return

    stubs.Set(qonos_client.Client, 'update_schedule',
            fake_qonos_client_update_schedule)

    def fake_scheduled_images_create_schedule(cls, req, server_id):
        return

    cls = scheduled_images.ScheduledImagesController
    stubs.Set(cls, '_create_image_schedule',
            fake_scheduled_images_create_schedule)
