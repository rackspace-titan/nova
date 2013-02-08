# Copyright 2013 Rackspace Hosting
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

from nova.api.openstack import compute
from nova.api.openstack.compute.contrib import scheduled_images
from nova.compute import api as compute_api
from nova.openstack.common import jsonutils
from nova import test
from nova.tests.api.openstack import fakes
from nova.tests import fake_scheduled_images


OS_SI = 'OS-SI:image_schedule'


class ScheduledImagesTest(test.TestCase):
    def setUp(self):
        super(ScheduledImagesTest, self).setUp()
        self.controller = scheduled_images.ScheduledImagesController()
        self.uuid_1 = 'b04ac9cd-f78f-4376-8606-99f3bdb5d0ae'
        self.uuid_2 = '6b8b2aa4-ae7b-4cd0-a7f9-7fa6d5b0195a'
        uuids = [self.uuid_1, self.uuid_2]
        fake_scheduled_images.stub_out_instance(self.stubs, uuids)
        fake_scheduled_images.stub_out_instance_system_metadata(self.stubs)
        fake_scheduled_images.stub_out_qonos_client(self.stubs)

    def test_get_image_schedule(self):
        url = '/fake/servers/%s/os-si-image-schedule' % self.uuid_1
        req = fakes.HTTPRequest.blank(url)
        res = self.controller.index(req, self.uuid_1)
        self.assertEqual(res, {"image_schedule": {"retention": "6"}})

    def test_post_image_schedule(self):
        url = '/fake/servers/%s/os-si-image-schedule' % self.uuid_1
        req = fakes.HTTPRequest.blank(url)
        body = {"image_schedule": {"retention": "7"}}
        res = self.controller.create(req, self.uuid_1, body)
        self.assertEqual(res, {"image_schedule": {"retention": "7"}})

    def test_delete_image_schedule(self):
        url = '/fake/servers/%s/os-si-image-schedule' % self.uuid_1
        req = fakes.HTTPRequest.blank(url)
        req.method = 'DELETE'
        res = self.controller.delete(req, self.uuid_1)
        self.assertEqual(res.status_int, 202)


class ScheduledImagesFilterTest(test.TestCase):
    def setUp(self):
        super(ScheduledImagesFilterTest, self).setUp()
        self.controller = scheduled_images.ScheduledImagesFilterController()
        self.uuid_1 = 'b04ac9cd-f78f-4376-8606-99f3bdb5d0ae'
        self.uuid_2 = '6b8b2aa4-ae7b-4cd0-a7f9-7fa6d5b0195a'
        uuids = [self.uuid_1, self.uuid_2]
        fake_scheduled_images.stub_out_instance(self.stubs, uuids)
        fake_scheduled_images.stub_out_instance_system_metadata(self.stubs)
        fake_scheduled_images.stub_out_qonos_client(self.stubs)
        self.app = compute.APIRouter(init_only=('servers'))

        def fake_delete(cls, context, id_):
            return

        self.stubs.Set(compute_api.API, 'delete', fake_delete)

    def assertScheduledImages(self, dict_, value, is_present=True):
        if is_present:
            self.assert_(OS_SI in dict_)
            self.assert_('retention' in dict_[OS_SI])
            self.assertEqual(dict_[OS_SI]['retention'], value)
        else:
            self.assert_(OS_SI not in dict_)

    def test_index_servers_with_true_query(self):
        query = 'OS-SI:image_schedule=True'
        req = fakes.HTTPRequest.blank('/fake/servers?%s' % query)
        res = req.get_response(self.app)
        servers = jsonutils.loads(res.body)['servers']
        for server in servers:
            self.assertScheduledImages(server, '6', is_present=True)

    def test_index_servers_with_false_query(self):
        query = 'OS-SI:image_schedule=False'
        req = fakes.HTTPRequest.blank('/fake/servers?%s' % query)
        res = req.get_response(self.app)
        servers = jsonutils.loads(res.body)['servers']
        for server in servers:
            self.assertScheduledImages(server, '6', is_present=False)

    def test_show_server(self):
        req = fakes.HTTPRequest.blank(
            '/fake/servers/%s' % self.uuid_1)
        res = req.get_response(self.app)
        server = jsonutils.loads(res.body)['server']
        self.assertScheduledImages(server, '6', is_present=True)

    def test_detail_servers(self):
        req = fakes.HTTPRequest.blank('/fake/servers/detail')
        res = req.get_response(self.app)
        servers = jsonutils.loads(res.body)['servers']
        for server in servers:
            if server['id'] == self.uuid_1:
                self.assertScheduledImages(server, '6', is_present=True)
            else:
                self.assertScheduledImages(server, '6', is_present=False)

    def test_detail_servers_with_true_query(self):
        query = 'OS-SI:image_schedule=True'
        req = fakes.HTTPRequest.blank('/fake/servers/detail?%s' % query)
        res = req.get_response(self.app)
        servers = jsonutils.loads(res.body)['servers']
        for server in servers:
            self.assertScheduledImages(server, '6', is_present=True)

    def test_detail_servers_with_false_query(self):
        query = 'OS-SI:image_schedule=False'
        req = fakes.HTTPRequest.blank('/fake/servers/detail?%s' % query)
        res = req.get_response(self.app)
        servers = jsonutils.loads(res.body)['servers']
        for server in servers:
            self.assertScheduledImages(server, '6', is_present=False)

    def test_delete_server(self):
        query = 'OS-SI:image_schedule=False'
        req = fakes.HTTPRequest.blank('/fake/servers/%s' % self.uuid_2)
        req.method = 'DELETE'
        res = req.get_response(self.app)
        self.assertEqual(res.status_int, 204)
