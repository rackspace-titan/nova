# vim: tabstop=4 shiftwidth=4 softtabstop=4

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


import random
import webob
from webob import exc

from nova.api.openstack import extensions
from nova.api.openstack import wsgi
from nova.api.openstack import xmlutil
from nova import compute
from nova import db as db_api
from nova import exception
from nova.openstack.common import jsonutils
from nova.openstack.common import log as logging
from oslo.config import cfg
from qonos.qonosclient import client
from qonos.qonosclient import exception as qonos_exc


ALIAS = 'os-si-image-schedule'
XMLNS_SI = 'http://docs.openstack.org/servers/api/ext/scheduled_images/v1.0'
XML_SI_PREFIX = 'OS-SI'
SI_METADATA_KEY = 'OS-SI:image_schedule'
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
        return xmlutil.SlaveTemplate(root, 1, nsmap={None: XMLNS_SI})


class ScheduledImagesController(wsgi.Controller):
    """Controller class for Scheduled Images."""

    def __init__(self):
        super(ScheduledImagesController, self).__init__()
        endpoint = CONF.qonos_service_api_endpoint
        port = CONF.qonos_service_port
        self.client = client.Client(endpoint, port)
        self.compute_api = compute.API()

    @wsgi.serializers(xml=ScheduledImagesTemplate)
    def index(self, req, server_id):
        """Returns the retention value for the schedule."""
        context = req.environ['nova.context']
        authorize(context)

        metadata = db_api.instance_system_metadata_get(context, server_id)
        if metadata.get(SI_METADATA_KEY):
            retention_str = metadata[SI_METADATA_KEY]
            retention = jsonutils.loads(retention_str)
        else:
            msg = _('Image schedule does not exist for server %s') % server_id
            raise exc.HTTPNotFound(explanation=msg)

        return {"image_schedule": retention}

    def delete(self, req, server_id):
        """Deletes a Schedule."""
        context = req.environ['nova.context']
        authorize(context)

        try:
            params = {'instance_id': server_id, 'action': 'snapshot'}
            schedules = self.client.list_schedules(filter_args=params)
        except qonos_exc.ConnRefused:
            LOG.warn(_('QonoS API unreachable while trying to list schedules'))

        if len(schedules) == 0:
            msg = (_('Image schedule does not exist for server %s')
                   % server_id)
            raise exc.HTTPNotFound(explanation=msg)

        try:
            for schedule in schedules:
                self.client.delete_schedule(schedule['id'])
        except qonos_exc.NotFound:
            msg = (_('Image schedule does not exist for server %s')
                   % server_id)
            raise exc.HTTPNotFound(explanation=msg)

        metadata = db_api.instance_system_metadata_get(context, server_id)
        if metadata.get(SI_METADATA_KEY):
            del metadata[SI_METADATA_KEY]
            metadata = db_api.instance_system_metadata_update(context,
                               server_id, metadata, True)

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
        """Validate the image schedule body."""
        if body.get('image_schedule') is None:
            msg = (_('The request body is invalid. Key image_schedule is '
                     'missing in the body'))
            raise exc.HTTPBadRequest(explanation=msg)
        elif body['image_schedule'].get('retention') is None:
            msg = (_('The request body is invalid. Key retention is '
                     'missing in the body'))
            raise exc.HTTPBadRequest(explanation=msg)

        retention = body['image_schedule']['retention']
        try:
            retention = int(retention)
        except ValueError:
            msg = (_('The retention value %s is not allowed. It must '
                     'be an integer') % retention)
            raise exc.HTTPBadRequest(explanation=msg)

        if retention <= 0:
            msg = (_('The retention value %s is not allowed. '
                     'It must be greater than 0') % retention)
            raise exc.HTTPBadRequest(explanation=msg)

        if CONF.qonos_retention_limit_max < retention:
            msg = (_('The retention value %(val)s is not allowed. '
                     'It cannot exceed %(max)s')
                    % {"val": retention,
                       "max": CONF.qonos_retention_limit_max})
            raise exc.HTTPBadRequest(explanation=msg)

        return {"retention": retention}

    @wsgi.serializers(xml=ScheduledImagesTemplate)
    def create(self, req, server_id, body):
        """Creates a new Schedule."""
        context = req.environ['nova.context']
        authorize(context)

        retention = self.is_valid_body(body)

        try:
            instance = db_api.instance_get_by_uuid(context, server_id)
        except exception.InstanceNotFound:
            msg = _('Specified instance %s could not be found.')
            raise exc.HTTPNotFound(msg % server_id)

        self._create_image_schedule(req, server_id)

        system_metadata = {}
        retention_str = jsonutils.dumps(retention)
        system_metadata[SI_METADATA_KEY] = retention_str
        try:
            system_metadata = db_api.instance_system_metadata_update(context,
                                      server_id, system_metadata, False)
        except exception.InstanceNotFound:
            msg = _('Specified instance %s could not be found.')
            raise exc.HTTPNotFound(msg % server_id)
        retention_str = system_metadata[SI_METADATA_KEY]
        retention = jsonutils.loads(retention_str)
        return {"image_schedule": retention}


class ServerScheduledImagesTemplate(xmlutil.TemplateBuilder):
    def construct(self):
        root = xmlutil.TemplateElement('server')
        img_sch = xmlutil.SubTemplateElement(root,
                          '{%s}image_schedule' % XMLNS_SI,
                          selector=SI_METADATA_KEY)
        retention = xmlutil.SubTemplateElement(img_sch, 'retention',
                          selector='retention')
        retention.text = int
        return xmlutil.SlaveTemplate(root, 1, nsmap={XML_SI_PREFIX: XMLNS_SI})


class ServersScheduledImagesTemplate(xmlutil.TemplateBuilder):
    def construct(self):
        root = xmlutil.TemplateElement('servers')
        elem = xmlutil.SubTemplateElement(root, 'server', selector='servers')
        img_sch = xmlutil.SubTemplateElement(elem,
                          '{%s}image_schedule' % XMLNS_SI,
                          selector=SI_METADATA_KEY)
        retention = xmlutil.SubTemplateElement(img_sch, 'retention',
                          selector='retention')
        retention.text = int
        return xmlutil.SlaveTemplate(root, 1, nsmap={XML_SI_PREFIX: XMLNS_SI})


class ScheduledImagesFilterController(wsgi.Controller):
    def __init__(self, *args, **kwargs):
        super(ScheduledImagesFilterController, self).__init__(*args, **kwargs)
        endpoint = CONF.qonos_service_api_endpoint
        port = CONF.qonos_service_port
        self.client = client.Client(endpoint, port)
        self.compute_api = compute.API()

    def _get_all_retention_values(self, req):
        """Returns a dict with instance_uuid: retention."""
        context = req.environ['nova.context']
        retention_values = db_api.instance_system_metadata_get_all_by_key(
                                        context, SI_METADATA_KEY)
        return retention_values

    def _check_si_opt(self, req):
        search_opts = {}
        search_opts.update(req.GET)
        if SI_METADATA_KEY in search_opts:
            search_opt = search_opts[SI_METADATA_KEY]
            if search_opt.lower() == 'true':
                return True
            elif search_opt.lower() == 'false':
                return False
            else:
                msg = _('Bad value for query parameter OS-SI:image_schedule, '
                        'use True or False')
                raise exc.HTTPBadRequest(explanation=msg)
        else:
            return None

    def _filter_servers_on_si(self, req, servers, must_have_si):
        if must_have_si is None:
            return

        retention_values = self._get_all_retention_values(req)
        for server in reversed(servers):
            if not (must_have_si == (server['id'] in retention_values)):
                servers.remove(server)

    def _add_si_metadata(self, req, servers):
        must_have_si = self._check_si_opt(req)
        self._filter_servers_on_si(req, servers, must_have_si)
        # Only add metadata to servers we know (may) have it
        if (must_have_si is None) or must_have_si:
            retention_values = self._get_all_retention_values(req)
            for server in servers:
                if server['id'] in retention_values:
                    si_meta_str = retention_values[server['id']]
                    si_meta = jsonutils.loads(si_meta_str)
                    server[SI_METADATA_KEY] = si_meta

    @wsgi.extends
    def index(self, req, resp_obj):
        context = req.environ['nova.context']
        if authorize_filter(context):
            resp_obj.attach(xml=ServersScheduledImagesTemplate())
            servers = resp_obj.obj['servers']
            self._add_si_metadata(req, servers)

    @wsgi.extends
    def show(self, req, resp_obj, id):
        context = req.environ['nova.context']
        if authorize_filter(context):
            resp_obj.attach(xml=ServerScheduledImagesTemplate())
            servers = [resp_obj.obj['server']]
            self._add_si_metadata(req, servers)

    @wsgi.extends
    def detail(self, req, resp_obj):
        context = req.environ['nova.context']
        if authorize_filter(context):
            resp_obj.attach(xml=ServersScheduledImagesTemplate())
            servers = resp_obj.obj['servers']
            self._add_si_metadata(req, servers)

    @wsgi.extends
    def delete(self, req, resp_obj, id):
        context = req.environ['nova.context']
        if resp_obj.code == 204 and authorize_filter(context):
            metadata = db_api.instance_system_metadata_get(context, id)
            if metadata.get(SI_METADATA_KEY):
                del metadata[SI_METADATA_KEY]
                metadata = db_api.instance_system_metadata_update(context,
                        id, metadata, True)
            params = {'action': 'snapshot', 'instance_id': id}
            try:
                schedules = self.client.list_schedules(filter_args=params)
                for schedule in schedules:
                    self.client.delete_schedule(schedule['id'])
            except qonos_exc.ConnRefused:
                msg = _('QonoS API is not reachable, delete on server did not '
                        'delete QonoS schedules')
                LOG.warn(msg)


class Scheduled_images(extensions.ExtensionDescriptor):
    """Enables automatic daily images to be taken of a server."""

    name = "ScheduledImages"
    alias = ALIAS
    namespace = XMLNS_SI
    updated = "2013-03-20T00:00:00+00:00"

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
