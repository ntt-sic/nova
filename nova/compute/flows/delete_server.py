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
from nova.openstack.common import timeutils
from nova.objects import base as obj_base
from nova.objects import instance as instance_obj
from nova.objects import quotas as quotas_obj
from nova.objects import service as service_obj
from nova import quota
from nova import utils


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


class DeleteServerManagerTask(task.Task):
    """Delete Server."""
    def __init__(self, nova_cpu, context, instance, bdms, reservations):
        super(DeleteServerManagerTask, self).__init__()
        self.nova_cpu = nova_cpu
        self.context = context
        self.instance = instance
        self.bdms =bdms
        self.reservations = reservations

    def execute(self):
        instance_uuid = self.instance['uuid']
        image = self.instance['image_ref']

        if self.context.is_admin and self.context.project_id != self.instance['project_id']:
            project_id = self.instance['project_id']
        else:
            project_id = self.context.project_id
        if self.context.user_id != self.instance['user_id']:
            user_id = self.instance['user_id']
        else:
            user_id = self.context.user_id

        was_soft_deleted = self.instance['vm_state'] == vm_states.SOFT_DELETED
        if was_soft_deleted:
            # Instances in SOFT_DELETED vm_state have already had quotas
            # decremented.
            try:
                self.nova_cpu._quota_rollback(self.context, self.reservations,
                                     project_id=project_id,
                                     user_id=user_id)
            except Exception:
                pass
            self.reservations = None

        try:
            db_inst = obj_base.obj_to_primitive(self.instance)
            self.nova_cpu.conductor_api.instance_info_cache_delete(self.context, db_inst)
            self.nova_cpu._notify_about_instance_usage(self.context, self.instance,
                                              "delete.start")
            self.nova_cpu._shutdown_instance(self.context, db_inst, self.bdms)
            # NOTE(vish): We have already deleted the instance, so we have
            #             to ignore problems cleaning up the volumes. It
            #             would be nice to let the user know somehow that
            #             the volume deletion failed, but it is not
            #             acceptable to have an instance that can not be
            #             deleted. Perhaps this could be reworked in the
            #             future to set an instance fault the first time
            #             and to only ignore the failure if the instance
            #             is already in ERROR.
            try:
                self.nova_cpu._cleanup_volumes(self.context, instance_uuid, self.bdms)
            except Exception as exc:
                err_str = _("Ignoring volume cleanup failure due to %s")
                LOG.warn(err_str % exc, instance=self.instance)
            # if a delete task succeed, always update vm state and task
            # state without expecting task state to be DELETING
            self.instance.vm_state = vm_states.DELETED
            self.instance.task_state = None
            self.instance.terminated_at = timeutils.utcnow()
            self.instance.save()
            system_meta = utils.instance_sys_meta(self.instance)
            db_inst = self.nova_cpu.conductor_api.instance_destroy(
                self.context, obj_base.obj_to_primitive(self.instance))
            instance = instance_obj.Instance._from_db_object(self.context,
                                                             self.instance,
                                                             db_inst)
        except Exception:
            with excutils.save_and_reraise_exception():
                self.nova_cpu._quota_rollback(self.context, self.reservations,
                                     project_id=project_id,
                                     user_id=user_id)

        quotas = quotas_obj.Quotas.from_reservations(self.context,
                                                     self.reservations,
                                                     instance=instance)
        self.nova_cpu._complete_deletion(self.context,
                                instance,
                                self.bdms,
                                quotas,
                                system_meta)


def get_engine_config():
    backend_config = {
        'connection': CONF.backend,
    }
    with contextlib.closing(backends.fetch(backend_config)) as be:
        with contextlib.closing(be.get_connection()) as conn:
            conn.upgrade()

    engine_config = {
        'backend': backend_config,
        'engine_conf': 'serial',
        'book': logbook.LogBook("delete-vm-api"),
    }

    return engine_config


def create_api_flow(nova_api, context, instance, delete_type, cb, instance_attrs):
    flow = lf.Flow("delete-vm-api")
    flow.add(
            DeleteServerAPITask(nova_api, context, instance, delete_type,
                                cb, instance_attrs),
        ),
    return flow


def create_manager_flow(nova_cpu, context, instance, bdms, reservations):
    flow = lf.Flow("delete-vm-mgr")
    flow.add(
            DeleteServerManagerTask(nova_cpu, context, instance, bdms, reservations),
        ),
    return flow


def api_flow(nova_api, context, instance, delete_type, cb, instance_attrs):

    import pdb; pdb.set_trace()

    flow = create_api_flow(nova_api, context, instance, delete_type, cb,
                                                   instance_attrs)
    engine_config = get_engine_config()
    engine = engines.load(flow, **engine_config)
    engine.run()
    return engine


def manager_flow(nova_cpu, context, instance, bdms, reservations):

    flow = create_manager_flow(nova_cpu, context, instance, bdms, reservations)
    engine_config = get_engine_config()
    engine = engines.load(flow, **engine_config)
    engine.run()
    return engine
