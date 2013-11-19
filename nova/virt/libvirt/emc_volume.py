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

"""Volume drivers for libvirt."""

import fnmatch
import os
import time

from nova import exception
from nova import flags
from nova.openstack.common import cfg
from nova.openstack.common import log as logging
from nova import utils
from nova.virt.libvirt import config
from nova.virt.libvirt import utils as virtutils
from nova.virt.libvirt.volume import LibvirtVolumeDriver

emc_volume_opts = [
    cfg.IntOpt('multipath_remove_retries',
        default=10,
        help='Retry count of removing multipath map'),
    cfg.IntOpt('multipath_list_retries',
        default=5,
        help='Retry count of listing multipath map'),
    cfg.IntOpt('multipath_cli_exec_timeout',
               default=30,
               help='more than 0, multipath-cli timeout function is' +\
                    ' effective'),
    cfg.IntOpt('udev_event_timeout',
        default=120,
        help='Seconds to wait udev events finished'),
]

LOG = logging.getLogger(__name__)
FLAGS = flags.FLAGS
FLAGS.register_opts(emc_volume_opts)
flags.DECLARE('num_iscsi_scan_tries', 'nova.volume.driver')


class LibvirtEMCISCSIVolumeDriver(LibvirtVolumeDriver):
    """Driver to attach Network volumes to libvirt."""

    def _run_iscsiadm_all(self, iscsi_properties, iscsi_command, **kwargs):
        """Execute command of iscsiadm to all target"""
        target_total_num = len(iscsi_properties['target_iqn'])
        failed_count = 0
        last_exc = None
        for target_num in range(target_total_num):
            try:
                self._run_iscsiadm_one(iscsi_properties, target_num,
                                       iscsi_command, **kwargs)
            except exception.ProcessExecutionError as exc:
                failed_count += 1
                last_exc = exc

        if target_total_num == failed_count:
            # failed to all target
            raise last_exc

    @staticmethod
    def _run_iscsiadm_one(iscsi_properties,
                          target_num, iscsi_command, **kwargs):
        """Execute command of iscsiadm to one target"""
        check_exit_code = kwargs.pop('check_exit_code', 0)

        if '--op' in iscsi_command and 'new' in iscsi_command\
        and iscsi_command.index('new') - iscsi_command.index('--op') == 1:
            (out, err) = utils.execute(
                'iscsiadm', '-m', 'node', '-T',
                iscsi_properties['target_iqn'][target_num],
                '-p', "%s,%s" % (iscsi_properties['target_portal'][target_num],
                iscsi_properties['target_portal_group_tag'][target_num]),
                *iscsi_command, run_as_root=True,
                check_exit_code=check_exit_code)
        else:
            (out, err) = utils.execute(
                'iscsiadm', '-m', 'node', '-T',
                iscsi_properties['target_iqn'][target_num],
                '-p', iscsi_properties['target_portal'][target_num],
                *iscsi_command, run_as_root=True,
                check_exit_code=check_exit_code)
        LOG.debug("iscsiadm %s (iscsi No.%d) : stdout=%s stderr=%s" %
                  (iscsi_command, target_num, out, err))
        return (out, err)

    def _iscsiadm_update(self, iscsi_properties, property_key, property_value,
                         **kwargs):
        iscsi_command = ('--op', 'update', '-n', property_key,
                         '-v', property_value)
        return self._run_iscsiadm_all(iscsi_properties,
                                      iscsi_command, **kwargs)

    def _change_multipath(self, path):
        """Change to the multipath from host device path"""
        # Identify the device file name
        link_path = []
        for i_path in path:
            link_path.append(os.path.realpath(i_path))

        # Get multipath name
        _, multipath_name = self._get_multipath_name_and_status(link_path)

        # Get multipath full path
        multipath_full_path = '/dev/mapper/' + multipath_name

        return multipath_full_path

    @staticmethod
    def _get_multipath_name_and_status(link_path):
        """Identify the multipath name and return path status"""
        device_file_name = []
        path_status = {}
        for i_path in link_path:
            device_file_name.append(i_path.rsplit('/', 1)[1])
            path_status[device_file_name[-1]] = False
        multipath_name = None

        for i in range(0, FLAGS.multipath_list_retries):
            time.sleep(i ** 2)
            # wait until udev events finished
            utils.execute('udevadm', 'settle',
                          '--timeout=%s' % FLAGS.udev_event_timeout,
                          run_as_root=True,
                          check_exit_code=[0, 1])

            # get multipath maps
            (out, err) = utils.execute('multipath', '-l',
                                        run_as_root=True)
            LOG.debug("multipath -l : stdout=%s stderr=%s" %
                        (out, err))

            # Check if the list of multipath exists
            if out == "":
                raise exception.NovaException(
                    _("list of multipath doesn't exist"))

            lines = out.splitlines()
            for device_file_name_index, line in enumerate(lines):
                for i_device in device_file_name:
                    if ' ' + i_device + ' ' in line:
                        # Update status if the path is active
                        path_status[i_device] = 'active' in line
                        if multipath_name is None:
                            for line in lines[device_file_name_index::-1]:
                                if 'dm-' in line:
                                    multipath_name = line.split(' ')[0]
                                    break
                        break
            if not multipath_name is None:
                break

            # Kick multipathd again.
            for i_path in link_path:
                utils.execute('multipath', i_path, run_as_root=True,
                              check_exit_code=False)

        else:
            raise exception.NovaException(
                _("multipath name( device file name=%s ) not found") %
                (device_file_name))

        return path_status, multipath_name

    @staticmethod
    def _get_existing_host_devices(iscsi_properties):
        """Get existing host device paths"""
        existing_host_devices = []
        for target_num in range(len(iscsi_properties['target_iqn'])):
            host_device = ("/dev/disk/by-path/ip-%s-iscsi-%s-lun-%s" %
                            (iscsi_properties['target_portal'][target_num],
                             iscsi_properties['target_iqn'][target_num],
                             iscsi_properties.get('target_lun', 0)))
            if os.path.exists(host_device):
                existing_host_devices.append(host_device)
            else:
                LOG.warn("/dev/disk/by-path/ip-%s-iscsi-%s-lun-%s was not found." %
                            (iscsi_properties['target_portal'][target_num],
                             iscsi_properties['target_iqn'][target_num],
                             iscsi_properties.get('target_lun', 0)))

        return existing_host_devices

    @utils.synchronized('connect_volume')
    def connect_volume(self, connection_info, mount_device, symlinkdir):
        """Attach the volume to instance_name"""
        iscsi_properties = connection_info['data']
        target_total_num = len(iscsi_properties['target_iqn'])
        # NOTE(vish): If we are on the same host as nova volume, the
        #             discovery makes the target so we don't need to
        #             run --op new. Therefore, we check to see if the
        #             target exists, and if we get 255 (Not Found), then
        #             we run --op new. This will also happen if another
        #             volume is using the same target.
        failed_count = 0
        last_exc = None
        for target_num in range(target_total_num):
            try:
                self._run_iscsiadm_one(iscsi_properties, target_num, ())
            except exception.ProcessExecutionError as exc:
                if exc.exit_code in [255]:
                    try:
                        self._run_iscsiadm_one(iscsi_properties,
                                               target_num, ('--op', 'new'))
                    except exception.ProcessExecutionError as exc:
                        failed_count += 1
                        last_exc = exc
                else:
                    failed_count += 1
                    last_exc = exc

        if failed_count == target_total_num:
            # failed to all target
            raise last_exc

        if iscsi_properties.get('auth_method'):
            self._iscsiadm_update(iscsi_properties,
                                  "node.session.auth.authmethod",
                                  iscsi_properties['auth_method'])
            self._iscsiadm_update(iscsi_properties,
                                  "node.session.auth.username",
                                  iscsi_properties['auth_username'])
            self._iscsiadm_update(iscsi_properties,
                                  "node.session.auth.password",
                                  iscsi_properties['auth_password'])

        # NOTE(vish): If we have another lun on the same target, we may
        #             have a duplicate login
        failed_count = 0
        last_exc = None
        loggedin_target = []
        (out, err) = utils.execute('iscsiadm', '-m', 'session',
                                   run_as_root=True,
                                   check_exit_code=[0, 21, 255])
        for target_num in range(target_total_num):
            if (iscsi_properties['target_iqn'][target_num] + "\n") in out:
                loggedin_target.append(target_num)
            else:
                try:
                    self._run_iscsiadm_one(iscsi_properties,
                                           target_num, ("--login",),
                                           check_exit_code=[0, 15])
                except exception.ProcessExecutionError as exc:
                    failed_count += 1
                    last_exc = exc

        if failed_count == target_total_num:
            # failed to all target
            raise last_exc

        self._iscsiadm_update(iscsi_properties, "node.startup", "automatic")

        # The /dev/disk/by-path/... node is not always present immediately
        # TODO(justinsb): This retry-with-delay is a pattern, move to utils?
        failed_count = 0
        last_exc = None

        # When it can be connected to one of targets, multipath can be made.
        # The processing to make host_devices recognition
        for target_num in loggedin_target:
            try:
                self._run_iscsiadm_one(iscsi_properties,
                                       target_num, ("--rescan",),
                                       check_exit_code=[0, 21, 255])
            except exception.ProcessExecutionError as exc:
                failed_count += 1
                last_exc = exc

        if failed_count == target_total_num:
            raise last_exc

        tries = 0
        existing_devices = None
        while True:
            # wait until udev events finished
            utils.execute('udevadm', 'settle',
                          '--timeout=%s' % FLAGS.udev_event_timeout,
                          run_as_root=True,
                          check_exit_code=[0, 1])

            existing_devices = self._get_existing_host_devices(
                iscsi_properties)
            if existing_devices:
                break

            if tries >= FLAGS.num_iscsi_scan_tries:
                raise exception.NovaException(_("iSCSI device not found"))

            LOG.info(_("ISCSI volume not yet found at: %(mount_device)s. "
                       "Will rescan & retry.  Try number: %(tries)s") %
                     locals())
            tries = tries + 1
            time.sleep(tries ** 2)

        # Change to multipath from host device path
        multipath_full_path = self._change_multipath(existing_devices)

        local_name = os.path.join(symlinkdir, os.path.basename(mount_device))
        sup = super(LibvirtEMCISCSIVolumeDriver, self)
        virtutils.ensure_symlink(multipath_full_path, local_name)
        connection_info['data']['device_path'] = local_name
        return sup.connect_volume(connection_info, mount_device, symlinkdir)

    @utils.synchronized('connect_volume')
    def disconnect_volume(self, connection_info, mount_device,
                          use_destination_info=False, force=False):
        chk_code = not force
        """Detach the volume from instance_name"""
        sup = super(LibvirtEMCISCSIVolumeDriver, self)
        sup.disconnect_volume(connection_info, mount_device,
                              use_destination_info, force)
        iscsi_properties = connection_info['data']
        if use_destination_info and 'dest_connection_info' in connection_info:
            iscsi_properties = connection_info['dest_connection_info']['data']
        # NOTE(vish): Only disconnect from the target if no luns from the
        #             target are in use.

        # wait until udev events finished.
        utils.execute('udevadm', 'settle',
                      '--timeout=%s' % FLAGS.udev_event_timeout,
                      run_as_root=True,
                      check_exit_code=chk_code)

        # When it can be connected to one of targets, multipath can be made.
        # Therefore only the one which exists is used.
        existing_devices = self._get_existing_host_devices(iscsi_properties)
        if not existing_devices:
            return

        # multipath_name is used at the time of elimination of multipath.
        link_path = []
        link_path_state = {}
        for i_device in existing_devices:
            link_path.append(os.readlink(i_device))

        try:
            link_path_state, multipath_name = \
                 self._get_multipath_name_and_status(link_path)

            # Flush page-cache before eliminate multipath.
            multipath_full_path = '/dev/mapper/' + multipath_name
            real_multi_path = os.path.realpath(multipath_full_path)
            utils.execute('blockdev', '--flushbufs', real_multi_path,
                          run_as_root=True, check_exit_code=chk_code)
            # multipath is eliminated.
            (out, err) = utils.execute('multipath', '-f', multipath_name,
                                       run_as_root=True,
                                       attempts=FLAGS.multipath_remove_retries,
                                       check_exit_code=chk_code,
                                       timeout=FLAGS.multipath_cli_exec_timeout)
        except Exception as exc:
            if force is False:
                raise exc

        # It's necessary to eliminate the link in the "/dev/disk/by-path" both.
        path_is_alive = False
        for path in existing_devices:
            # If "the path is not active" or "force=True", proceed flush and delete.
            i_chk_code = [0]
            device_link = os.readlink(path).rsplit('/', 1)[1]
            # don't check_exit_code if the path is dead
            path_simlink_state = link_path_state.get(device_link, False)
            i_chk_code = path_simlink_state
            if path_simlink_state is True:
                path_is_alive = True
            if force is True:
                i_chk_code = chk_code

            utils.execute('blockdev', '--flushbufs', path,
                          run_as_root=True, check_exit_code=i_chk_code)
            (dirname, drivename) = os.path.split(os.path.realpath(path))
            sysfs_path = "/sys/block/%s/device/delete" % drivename
            if os.path.exists(sysfs_path):
                (out, err) = utils.execute('dd', 'of=%s' % sysfs_path,
                                           process_input='1', run_as_root=True,
                                           check_exit_code=i_chk_code)
        if not path_is_alive and not force:
            raise exception.NovaException(
                _("all path of %s were not active.") % (multipath_name))

        existing_block_devices = []
        pathes = os.listdir('/dev/disk/by-path')
        for target_num in range(len(iscsi_properties['target_iqn'])):
            pattern = ("ip-%s-iscsi-%s-lun-" %
                (iscsi_properties['target_portal'][target_num],
                 iscsi_properties['target_iqn'][target_num]))
            existing_block_devices.extend(
                fnmatch.filter(pathes, pattern + '[1-9]*'))
        LOG.debug("existing block devices : %s " % (existing_block_devices))
        if not existing_block_devices:
            i_chk_code = [0, 21, 255]
            if force is True:
                i_chk_code = chk_code
            self._run_iscsiadm_all(iscsi_properties, ("--logout",),
                                   check_exit_code=i_chk_code)
            self._run_iscsiadm_all(iscsi_properties, ('--op', 'delete'),
                                   check_exit_code=i_chk_code)

    def rescan_volume(self, connection_info, mount_device):
        pass
