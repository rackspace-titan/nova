<?xml version='1.0' encoding='UTF-8'?>
<servers xmlns:OS-SI="http://docs.openstack.org/servers/api/ext/scheduled_images/v1.0" xmlns:atom="http://www.w3.org/2005/Atom" xmlns="http://docs.openstack.org/compute/api/v1.1">
  <server name="new-server-test" id="%(id)s">
    <OS-SI:image_schedule>
        <retention>%(int)s</retention>
    </OS-SI:image_schedule>
    <atom:link href="%(host)s/v2/openstack/servers/%(id)s" rel="self"/>
    <atom:link href="%(host)s/openstack/servers/%(id)s" rel="bookmark"/>
  </server>
</servers>
