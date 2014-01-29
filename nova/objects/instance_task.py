#    Copyright 2013 IBM Corp.
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

from nova import db
from nova import exception
from nova.objects import base
from nova.objects import fields
from nova.openstack.common import timeutils


class InstanceTask(base.NovaPersistentObject, base.NovaObject):
    # Version 1.0: Initial version
    VERSION = '1.0'

    fields = {
        'id': fields.IntegerField(nullable=False),
        'name': fields.StringField(),
        'state': fields.StringField(),
        'uuid': fields.UUID(nullable=False),
        'instance_uuid': fields.UUID(),
        'tag': fields.StringField(nullable=False),
        'user_id': fields.StringField(nullable=False),
        'project_id': fields.StringField(nullable=False),
        'error_message': fields.StringField(),
        'start_time': fields.DateTimeField(),
        'finish_time': fields.DateTimeField(),
        }

    @staticmethod
    def _from_db_object(context, task, db_migration):
        for key in task.fields:
            task[key] = db_migration[key]
        task._context = context
        task.obj_reset_changes()
        return task

    @base.remotable_classmethod
    def get_by_id(cls, context, task_id):
        db_task = db.task_get(context, task_id)
        return cls._from_db_object(context, cls(), db_task)

    @base.remotable
    def create(self, context):
        if self.obj_attr_is_set('id'):
            raise exception.ObjectActionError(action='create',
                    reason='already created')
        self.user_id = context.user_id
        self.project_id = context.project_id
        self.start_time = timeutils.utcnow()
        updates = self.obj_get_changes()
        updates.pop('id', None)
        db_inst_task = db.instance_task_create(context, updates)
        InstanceTask._from_db_object(context, self, db_inst_task)

    @base.remotable
    def save(self, context):
        updates = self.obj_get_changes()
        task = db.instance_task_update(context, self.id, updates)
        self._from_db_object(context, self, task)
