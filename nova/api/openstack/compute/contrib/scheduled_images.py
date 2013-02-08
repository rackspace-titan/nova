# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2013 OpenStack Foundation.
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


import random
import webob
from webob import exc

from nova.api.openstack import extensions
from nova.api.openstack import wsgi
from nova.api.openstack import xmlutil
from nova import compute
from nova import db as db_api
from nova.openstack.common import log as logging
from oslo.config import cfg
from qonos.common import exception
from qonos.qonosclient import client


ALIAS = 'os-si-image-schedule'
XMLNS_SI = 'http://docs.openstack.org/servers/api/ext/scheduled_images/v1.0'
LOG = logging.getLogger(__name__)
authorize = extensions.extension_authorizer('compute', 'scheduled_images')
authorize_filter = extensions.soft_extension_authorizer('compute',
                           'scheduled_images_filter')
scheduled_images_opts = [
    cfg.StrOpt("qonos_service_api_endpoint",
               default="localhost",
               help="endpoint to hit the QonoS service API."),
    cfg.IntOpt("qonos_service_port",
               default=8080,
               help="active port of the QonoS service."),
    cfg.IntOpt("qonos_retention_limit_max",
               default=30,
               help="maximum allowed retention by the QonoS service."),
]

CONF = cfg.CONF
CONF.register_opts(scheduled_images_opts)


class ScheduledImagesTemplate(xmlutil.TemplateBuilder):
    def construct(self):
        root = xmlutil.TemplateElement('image_schedule')
        elem = xmlutil.SubTemplateElement(root, 'retention',
                                          selector='image_schedule',
                                          subselector='retention')
        elem.text = int
        return xmlutil.SlaveTemplate(root, 1, nsmap={'OS-SI': XMLNS_SI})


class ScheduledImagesController(wsgi.Controller):
    """Controller class for Scheduled Images."""

    def __init__(self):
        endpoint = CONF.qonos_service_api_endpoint
        port = CONF.qonos_service_port
        self.client = client.Client(endpoint, port)
        self.compute_api = compute.API()
        super(ScheduledImagesController, self).__init__()

    @wsgi.serializers(xml=ScheduledImagesTemplate)
    def index(self, req, server_id):
        """Returns the retention value for the schedule."""
        context = req.environ['nova.context']
        authorize(context)

        metadata = db_api.instance_system_metadata_get(context, server_id)
        if metadata.get('OS-SI:image_schedule'):
            retention = metadata['OS-SI:image_schedule']
        else:
            msg = 'Image schedule does not exist for this server'
            raise exc.HTTPNotFound(explanation=msg)

        return {"image_schedule": retention}

    def delete(self, req, server_id):
        """Deletes a Schedule."""
        context = req.environ['nova.context']
        authorize(context)

        try:
            params = {'instance_id': server_id}
            schedules = self.client.list_schedules(filter_args=params)

            if len(schedules) == 0:
                raise exc.HTTPNotFound()

            self.client.delete_schedule(schedules[0]['id'])
            metadata = db_api.instance_system_metadata_get(context, server_id)
            if metadata.get('OS-SI:image_schedule'):
                del metadata['OS-SI:image_schedule']
                metadata = db_api.instance_system_metadata_update(context,
                                   server_id, metadata, True)
        except exception.NotFound:
            raise exc.HTTPNotFound()

        return webob.Response(status_int=202)

    def _create_image_schedule(self, req, server_id):
        tenant = req.environ['HTTP_X_TENANT_NAME']
        params = {'action': 'snapshot', 'instance_id': server_id}
        schedules = self.client.list_schedules(filter_args=params)
        sch_body = {}
        body_metadata = {"instance_id": server_id}
        body_schedule = {
                            "tenant": tenant,
                            "action": "snapshot",
                            "minute": int(random.uniform(0, 59)),
                            "hour": int(random.uniform(0, 23)),
                            "metadata": body_metadata,
                        }
        sch_body['schedule'] = body_schedule
        if len(schedules) == 0:
            schedule = self.client.create_schedule(sch_body)
        elif len(schedules) == 1:
            schedule = self.client.update_schedule(schedules[0]['id'],
                                                   sch_body)
        else:
            #Note(nikhil): an instance can have at max one schedule
            raise exc.HTTPInternalServerError()

    def is_valid_body(self, body):
        "Validate the image schedule body."
        try:
            retention = int(body['image_schedule']['retention'])
        except ValueError():
            msg = 'The retention value must be an integer'
            raise exc.HTTPBadRequest(explanation=msg)
        if retention <= 0:
            msg = 'The retention value must be greater than 0'
            raise exc.HTTPBadRequest(explanation=msg)
        if CONF.qonos_retention_limit_max < retention:
            msg = ('The retention value cannot exceed %s' %
                   CONF.qonos_retention_limit_max)
            raise exc.HTTPBadRequest(explanation=msg)

        return retention

    @wsgi.serializers(xml=ScheduledImagesTemplate)
    def create(self, req, server_id, body):
        """Creates a new Schedule."""
        context = req.environ['nova.context']
        authorize(context)

        try:
            retention = self.is_valid_body(body)
        except Exception as e:
            raise exc.HTTPBadRequest(e)

        #Raise Not Found if the instance cannot be found
        try:
            instance = db_api.instance_get_by_uuid(context, server_id)
        except Exception:
            raise exc.HTTPNotFound("Specified instance could not be found.")

        try:
            self._create_image_schedule(req, server_id)
        except Exception:
            raise exc.HTTPInternalServerError()

        system_metadata = {}
        system_metadata['OS-SI:image_schedule'] = retention
        system_metadata = db_api.instance_system_metadata_update(context,
                           server_id, system_metadata, False)
        retention = system_metadata['OS-SI:image_schedule']
        return {"image_schedule": retention}


class ServerScheduledImagesTemplate(xmlutil.TemplateBuilder):
    def construct(self):
        root = xmlutil.TemplateElement('server')
        img_sch = xmlutil.SubTemplateElement(root,
                          '{%s}image_schedule' % XMLNS_SI,
                          selector='OS-SI:image_schedule')
        retention = xmlutil.SubTemplateElement(img_sch, 'retention',
                          selector='retention')
        retention.text = int
        return xmlutil.SlaveTemplate(root, 1, nsmap={'OS-SI': XMLNS_SI})


class ServersScheduledImagesTemplate(xmlutil.TemplateBuilder):
    def construct(self):
        root = xmlutil.TemplateElement('servers')
        elem = xmlutil.SubTemplateElement(root, 'server', selector='servers')
        img_sch = xmlutil.SubTemplateElement(elem,
                          '{%s}image_schedule' % XMLNS_SI,
                          selector='OS-SI:image_schedule')
        retention = xmlutil.SubTemplateElement(img_sch, 'retention',
                          selector='retention')
        retention.text = int
        return xmlutil.SlaveTemplate(root, 1, nsmap={'OS-SI': XMLNS_SI})


class ScheduledImagesFilterController(wsgi.Controller):
    def __init__(self, *args, **kwargs):
        endpoint = CONF.qonos_service_api_endpoint
        port = CONF.qonos_service_port
        self.client = client.Client(endpoint, port)
        super(ScheduledImagesFilterController, self).__init__(*args, **kwargs)
        self.compute_api = compute.API()

    def _look_up_metadata(self, req, server_id):
        context = req.environ['nova.context']
        metadata = db_api.instance_system_metadata_get(context, server_id)
        return metadata

    def _add_si_metadata(self, req, servers):
        search_opts = {}
        search_opts.update(req.GET)
        if 'OS-SI:image_schedule' in search_opts:
            search_opt = search_opts['OS-SI:image_schedule']
            if search_opt.lower() == 'true':
                index = 0
                while index < len(servers):
                    server = servers[index]
                    metadata = self._look_up_metadata(req, server['id'])
                    if not metadata.get('OS-SI:image_schedule'):
                        del servers[index]
                    else:
                        si_meta = metadata['OS-SI:image_schedule']
                        server['OS-SI:image_schedule'] = si_meta
                        index += 1
            elif search_opt.lower() == 'false':
                index = 0
                while index < len(servers):
                    server = servers[index]
                    metadata = self._look_up_metadata(req, server['id'])
                    if metadata.get('OS-SI:image_schedule'):
                        del servers[index]
                    else:
                        index += 1
            else:
                msg = ('Bad value for query parameter OS-SI:image_schedule ,'
                       'use True or False')
                raise exc.HTTPBadRequest(explanation=msg)
        else:
            for server in servers:
                metadata = self._look_up_metadata(req, server['id'])
                if metadata.get('OS-SI:image_schedule'):
                    si_meta = metadata['OS-SI:image_schedule']
                    server['OS-SI:image_schedule'] = si_meta

    @wsgi.extends
    def index(self, req, resp_obj):
        context = req.environ['nova.context']
        if 'servers' in resp_obj.obj and authorize_filter(context):
            resp_obj.attach(xml=ServersScheduledImagesTemplate())
            servers = resp_obj.obj['servers']
            self._add_si_metadata(req, servers)
        else:
            LOG.info("Failed authorization for index in scheduled images")

    @wsgi.extends
    def show(self, req, resp_obj, id):
        context = req.environ['nova.context']
        if authorize_filter(context):
            resp_obj.attach(xml=ServerScheduledImagesTemplate())
            servers = [resp_obj.obj['server']]
            self._add_si_metadata(req, servers)
        else:
            LOG.info("Failed authorization for show in scheduled images")

    @wsgi.extends
    def detail(self, req, resp_obj):
        context = req.environ['nova.context']
        if 'servers' in resp_obj.obj and authorize_filter(context):
            resp_obj.attach(xml=ServersScheduledImagesTemplate())
            servers = resp_obj.obj['servers']
            self._add_si_metadata(req, servers)
        else:
            LOG.info("Failed authorization for detail in scheduled images")

    @wsgi.extends
    def delete(self, req, resp_obj, id):
        context = req.environ['nova.context']
        if resp_obj.code == 204 and authorize_filter(context):
            metadata = self._look_up_metadata(req, id)
            if metadata.get('OS-SI:image_schedule'):
                del metadata['OS-SI:image_schedule']
                metadata = db_api.instance_system_metadata_update(context,
                        id, metadata, True)
            params = {'action': 'snapshot', 'instance_id': id}
            schedules = self.client.list_schedules(filter_args=params)
            for schedule in schedules:
                self.client.delete_schedule(schedule['id'])
        else:
            LOG.info("Failed authorization for delete in scheduled images")


class Scheduled_images(extensions.ExtensionDescriptor):
    """Enables automatic daily images to be taken of a server."""

    name = "ScheduledImages"
    alias = ALIAS
    namespace = XMLNS_SI
    updated = "2013-03-19T00:00:00+00:00"

    def get_resources(self):
        ext = extensions.ResourceExtension('os-si-image-schedule',
                      ScheduledImagesController(),
                      collection_actions={'delete': 'DELETE'},
                      parent=dict(
                                      member_name='server',
                                      collection_name='servers',
                                 ))
        return [ext]

    def get_controller_extensions(self):
        controller = ScheduledImagesFilterController()
        extension = extensions.ControllerExtension(self, 'servers', controller)
        return [extension]
