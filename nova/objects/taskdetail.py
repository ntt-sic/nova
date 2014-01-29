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
from nova.objects import base
from nova.objects import fields


class TaskDetail(base.NovaPersistentObject, base.NovaObject):
    # Version 1.0: Initial version
    VERSION = '1.0'

    fields = {
        'parent_uuid': fields.StringField(nullable=True),
        'meta': fields.StringField(nullable=True),
        'name': fields.StringField(nullable=True),
        'results': fields.StringField(nullable=True),
        'version': fields.StringField(nullable=True),
        'state': fields.StringField(nullable=True),
        'uuid': fields.StringField(nullable=False),
        'failure': fields.StringField(nullable=True),
        }

    @staticmethod
    def _from_db_object(context, taskdetails, db_task_details):
        for details in taskdetails.fields:
            taskdetails[details] = db_task_details[details]
        taskdetails._context = context
        taskdetails.obj_reset_changes()
        return taskdetails

    @base.remotable_classmethod
    def get_by_state(cls, context, state):
        db_task_details = db.taskdetail_get_by_state(context, state)
        return cls._from_db_object(context, cls(), db_task_details)

    @base.remotable_classmethod
    def update_task_state(cls, conttext, state):

    #@base.remotable
    #def create(self, context):
    #    updates = self.obj_get_changes()
    #    db_task_details = db.task_details_create(context, updates)
    #    self._from_db_object(context, self, db_task_details)
