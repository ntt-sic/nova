#    Copyright 2014 Rackspace US
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
from nova.objects import base
from nova.objects import fields


class InstanceTask(base.NovaPersistentObject, base.NovaObject):
    # Version 1.0: Initial version
    VERSION = '1.0'

    fields = {
        'id': fields.IntegerField(),
        'task': fields.StringField(),
        'state': fields.StringField(),
        'uuid': fields.UUID(),
        'instance_uuid': fields.UUID(),
        'tag': fields.StringField(nullable=False),
        'user_id': fields.StringField(nullable=False),
        'project_id': fields.StringField(nullable=False),
        'start_time': fields.DateTimeField(nullable=False),
        'finish_time': fields.DateTimeField(nullable=True),
        }

    @staticmethod
    def _from_db_object(context, task, db_migration):
        for key in task.fields:
            task[key] = db_migration[key]
        task._context = context
        task.obj_reset_changes()
        return task

    @base.remotable_classmethod
    def get_by_instance_and_uuid(cls, context, instance_uuid, task_uuid):
        db_task = db.instance_task_get_by_instance_and_uuid(context,
                instance_uuid, task_uuid)
        return cls._from_db_object(context, cls(), db_task)

    @base.remotable
    def create(self, context):
        updates = self.obj_get_changes()
        updates.pop('id', None)
        db_inst_task = db.instance_task_create(context, updates)
        InstanceTask._from_db_object(context, self, db_inst_task)

    @base.remotable
    def save(self, context):
        updates = self.obj_get_changes()
        task = db.instance_task_update(context, self.uuid, updates)
        self._from_db_object(context, self, task)


class InstanceTaskList(base.ObjectListBase, base.NovaObject):
    # Version 1.0: Initial version
    #              InstanceTask <= 1.0
    VERSION = '1.0'
    fields = {
            'objects': fields.ListOfObjectsField('InstanceTask'),
            }
    child_versions = {
            '1.0': '1.0',
            }

    @base.remotable_classmethod
    def get_by_instance_uuid(cls, context, instance_uuid):
        db_tasks = db.instance_tasks_get_by_instance_uuid(context,
                instance_uuid)
        return base.obj_make_list(context, cls(), InstanceTask, db_tasks)

    @base.remotable_classmethod
    def get_by_filters(cls, context, filters):
        db_tasks = db.instance_tasks_get_by_filters(context, filters)
        return base.obj_make_list(context, cls(), InstanceTask, db_tasks)
