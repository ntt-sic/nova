# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright(c)2012-2013 NTT corp. All Rights Reserved.
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

import contextlib

from taskflow.patterns import linear_flow as lf

from taskflow import engines
from taskflow import task

from taskflow.persistence import backends
from taskflow.persistence import logbook

from oslo.config import cfg

from nova import exception
from nova import block_device
from nova.compute import instance_actions
from nova.compute import utils as compute_utils
from nova.compute import vm_states
from nova.openstack.common import excutils
from nova.openstack.common import log as logging
from nova.objects import service as service_obj
from nova import quota


LOG = logging.getLogger(__name__)

taskflow_opts = [
    cfg.StrOpt('backend',
               default='mysql://root:mysql@127.0.0.1/nova?charset=utf8',
               help=''),
]
CONF = cfg.CONF
CONF.register_opts(taskflow_opts)

QUOTAS = quota.QUOTAS


class DeleteServerAPITask(task.Task):
    """Delete Server."""
    def __init__(self, nova_api, context, instance, delete_type,
                 cb, instance_attrs):
        super(DeleteServerAPITask, self).__init__()
        self.nova_api=nova_api
        self.context = context
        self.instance = instance
        self.delete_type = delete_type
        self.cb = cb
        self.instance_attrs = instance_attrs

    def execute(self):
        host = self.instance['host']
        bdms = block_device.legacy_mapping(
                    self.nova_api.db.block_device_mapping_get_all_by_instance(
                    self.context, self.instance['uuid']))
        reservations = None

        if self.context.is_admin and self.context.project_id != self.instance['project_id']:
            project_id = self.instance['project_id']
        else:
            project_id = self.context.project_id
        if self.context.user_id != self.instance['user_id']:
            user_id = self.instance['user_id']
        else:
            user_id = self.context.user_id

        try:
            self.instance.update(self.instance_attrs)
            self.instance.progress = 0
            self.instance.save()
            new_type_id = self.instance.instance_type_id

            reservations = self.nova_api._create_reservations(self.context,
                                                     self.instance,
                                                     new_type_id,
                                                     project_id, user_id)

            if self.nova_api.cell_type == 'api':
                self.cb(self.context, self.instance, bdms, reservations=None)
                if reservations:
                    QUOTAS.commit(self.context,
                                  reservations,
                                  project_id=project_id,
                                  user_id=user_id)
                return

            if not host:
                try:
                    compute_utils.notify_about_instance_usage(
                            self.nova_api.notifier, self.context, self.instance,
                            "%s.start" % self.delete_type)
                    self.instance.destroy()
                    compute_utils.notify_about_instance_usage(
                            self.nova_api.notifier, self.context, self.instance,
                            "%s.end" % self.delete_type,
                            system_metadata=self.instance.system_metadata)
                    if reservations:
                        QUOTAS.commit(self.context,
                                      reservations,
                                      project_id=project_id,
                                      user_id=user_id)
                    return
                except exception.ObjectActionError:
                    self.instance.refresh()

            if self.instance['vm_state'] == vm_states.RESIZED:
                self.nova_api._confirm_resize_on_deleting(self.context, self.instance)

            is_up = False
            try:
                service = service_obj.Service.get_by_compute_host(
                    self.context.elevated(), self.instance.host)
                if self.nova_api.servicegroup_api.service_is_up(service):
                    is_up = True

                    self.nova_api._record_action_start(self.context, self.instance,
                                              instance_actions.DELETE)

                    self.cb(self.context, self.instance, bdms, reservations=reservations)
            except exception.ComputeHostNotFound:
                pass

            if not is_up:
                self.nova_api._local_delete(self.context, self.instance, bdms, self.delete_type, self.cb)
                if reservations:
                    QUOTAS.commit(self.context,
                                  reservations,
                                  project_id=project_id,
                                  user_id=user_id)
                    reservations = None
        except exception.InstanceNotFound:
            if reservations:
                QUOTAS.rollback(self.context,
                                reservations,
                                project_id=project_id,
                                user_id=user_id)
        except Exception:
            with excutils.save_and_reraise_exception():
                if reservations:
                    QUOTAS.rollback(self.context,
                                    reservations,
                                    project_id=project_id,
                                    user_id=user_id)


def create_flow(nova_api, context, instance, delete_type, cb, instance_attrs):
    flow = lf.Flow("delete_vm")
    flow.add(
            DeleteServerAPITask(nova_api, context, instance, delete_type,
                                cb, instance_attrs),
        ),
    return flow


def api_flow(nova_api, context, instance, delete_type, cb, instance_attrs):

    import pdb; pdb.set_trace()

    backend_config = {
        'connection': CONF.backend,
    }
    with contextlib.closing(backends.fetch(backend_config)) as be:
        with contextlib.closing(be.get_connection()) as conn:
            conn.upgrade()

    engine_config = {
        'backend': backend_config,
        'engine_conf': 'serial',
        'book': logbook.LogBook("my-test"),
    }

    flow = create_flow(nova_api, context, instance, delete_type, cb,
                                                   instance_attrs)
    engine = engines.load(flow, **engine_config)
    engine.run()
    return engine
