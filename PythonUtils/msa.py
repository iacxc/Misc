from __future__ import print_function

import json
import six
import time
import telnetlib
import xml.etree.ElementTree as ET


class MsaException(Exception):
    pass


class MsaObject(type):
    def __new__(cls, name, bases, dct):
        for attr in dct.get('__attrs__', ()):
            dct[attr] = None
            if attr[0] == '_':

                def getter(self, attr=attr):
                    return getattr(self, attr)

                dct[attr[1:]] = property(getter)

        return super(MsaObject, cls).__new__(cls, name, bases, dct)


@six.add_metaclass(MsaObject)
class Volume(object):
    """ Volume """
    __attrs__ = ('_name', '_size', '_lun', '_access', '_ports')

    def __init__(self, name, size, lun='0', access='rw', ports=''):
        self._name = name
        self._size = size
        self._lun = lun
        self._access = access
        self._ports = ports

    def __str__(self):
        return '<Volume: %s (size: %s, lun: %s, access: %s, ports: %s)>' % (
            self._name, self._size, self._lun, self._access, self._ports)

    def set_lun(self, lun):
        self._lun = lun

    def set_access(self, access):
        self._access = access

    def set_ports(self, ports):
        self._ports = ports


@six.add_metaclass(MsaObject)
class VDisk(object):
    """ VDisk """
    __attrs__ = ('_name', '_raid', '_diskcount', '_controller', '_slots',
                 '_volumes')

    def __init__(self, name, controller, raid='NRAID', diskcount=1,
                 slots=None):
        self._name = name
        self._controller = controller
        self._raid = raid
        self._diskcount = diskcount

        self._slots = [] if slots is None else Gslots.split(',')
        self._volumes = []

    def __str__(self):

        return ('<VDisk: %s '
                '(controller: %s, raid: %s, '
                'diskcount: %s, slots: [%s])>\n    %s') % (
                    self._name, self._controller, self._raid, self._diskcount,
                    ','.join(self._slots),
                    '\n    '.join(str(volume) for volume in self._volumes))

    def add_slot(self, slot):
        self._slots.append(slot)
        return self._slots

    def add_volume(self, volume):
        self._volumes.append(volume)
        return self._volumes

    def remove_volumes(self, volnames):
        self._volumes = [
            vol for vol in self._volumes if not vol.name in volnames
        ]
        return self._volumes


class MSA2000(telnetlib.Telnet, object):
    username = 'manage'
    password = '!manage'
    prompt = '# '

    def __init__(self, host):
        telnetlib.Telnet.__init__(self, host)
        self._host = host
        self._vdisks = []
        self._login()

    def _error(self, msg):
        raise MsaException('%s [ %s ]' % (msg, self._host))

    def _send(self, msg, echo=False):
        if __debug__ or echo:
            print('Send:', msg)

        self.write(msg + '\n')

    def _receive(self, prompt=None):
        if prompt is None:
            prompt = self.prompt

        output = self.read_until(prompt)

        if __debug__:
            print('Got:', output.strip())

        return output

    def _login(self):
        self._receive('login: ')
        self._send(self.username)

        self._receive('Password: ')
        self._send(self.password)

        self._receive()

        self._run('set cli-parameters api pager off brief on timeout 3600')

    def _run(self, cmd, echo=False):
        self._send(cmd, echo)
        output = self._receive()[:-2]  # skip the prompt
        if not output.startswith('<?xml'):
            output = '\n'.join(output.split('\n')[1:])

        xmltree = ET.fromstring(output)
        objects = []
        for elem in xmltree.findall('OBJECT'):
            props = dict((prop.get('name'), prop.text)
                         for prop in elem.findall('PROPERTY'))

            if elem.get('basetype') == 'status':
                if props['return-code'].strip() != '0':
                    self._error(props['response'])

            else:
                objects.append({
                    'type': elem.get('basetype'),
                    'property': props
                })

        return objects

    def _get_disks(self, name='all'):
        return self._run('show disks %s' % name)

    def _get_vdisks(self, name=''):
        return self._run('show vdisks %s' % name)

    def _get_volumes(self):
        return self._run('show volumes')

    def _get_volume_maps(self):
        return self._run('show volume-maps')

    def _delete_vdisks(self, vdisks=[]):
        if len(vdisks) == 0:
            return

        cmd = 'delete vdisks prompt yes %s' % ','.join(vdisks)
        return self._run(cmd)

    def _delete_volumes(self, vdname, volnames=[]):
        if len(volnames) == 0:
            return

        cmd = 'delete volumes %s' % ','.join(volnames)
        return self._run(cmd)

    def _create_vdisk(self, name, controller, raid, slots):
        cmd = 'create vdisk level %s disks %s assigned-to %s %s' % (
            raid, slots, controller, name)

        return self._run(cmd)

    def _create_volume(self, vdname, name, size, access, lun, ports):
        cmd = ('create volume'
               ' vdisk %s size %s access %s lun %s ports %s %s') % (
                   vdname, size, access, lun, ports, name)

        return self._run(cmd)

    def _scan(self):
        if len(self._vdisks) > 0:
            return

        for vdisk_entry in self._get_vdisks():
            vdisk = VDisk(vdisk_entry['property']['name'],
                          vdisk_entry['property']['owner'],
                          vdisk_entry['property']['raidtype'],
                          vdisk_entry['property']['diskcount'])

            for disk_entry in self._get_disks('vdisk %s' % vdisk.name):
                vdisk.add_slot(disk_entry['property']['location'])

            self._vdisks.append(vdisk)

        if len(self._vdisks) == 0:  # no vdisk
            return

        vol_ref = {}
        for vol_entry in self._get_volumes():
            volume = Volume(vol_entry['property']['volume-name'],
                            vol_entry['property']['size'])
            vol_ref[volume.name] = volume

            vdname = vol_entry['property'].get('virtual-disk-name')
            if vdname:
                for vdisk in self._vdisks:
                    if vdisk.name == vdname:
                        vdisk.add_volume(volume)

        if len(vol_ref) == 0:
            return

        cur_vol = None
        for vol_map in self._get_volume_maps():
            if vol_map['type'] == 'volume-view':
                if vol_map['property'].get('volume-name'):
                    cur_vol = vol_ref[vol_map['property']['volume-name']]

            elif vol_map['type'] == 'volume-view-mappings':
                cur_vol.set_lun(vol_map['property']['lun'])
                cur_vol.set_access(vol_map['property']['access'])
                cur_vol.set_ports(vol_map['property']['ports'])

    def find_vdisk(self, vdname):
        """ find the vdisk for a specific name """
        self._scan()
        for vdisk in self._vdisks:
            if vdisk.name == vdname:
                return vdisk
        return None

    def delete_vdisks(self, vdnames):
        for vdname in vdnames:
            if self.find_vdisk(vdname) is None:
                raise MsaException('Vdisk "%s" not exists' % vdname)

        self._delete_vdisks(vdnames)
        self._vdisks = [
            vdisk for vdisk in self._vdisks if not vdisk.name in vdnames
        ]

    def delete_volumes(self, vdname, volnames):
        vdisk = self.find_vdisk(vdname)
        if vdisk is None:
            raise MsaException('Vdisk "%s" not exist' % vdname)

        self._delete_volumes(vdname, volnames)
        vdisk.remove_volumes(volnames)

    def create_vdisks(self, vdconfs):
        for conf in vdconfs:
            if self.find_vdisk(conf['name']):
                raise MsaException('Vdisk "%s" exists, cannot create' %
                                   conf['name'])

            self._create_vdisk(**conf)
            self._vdisks.append(VDisk(conf['name'], conf['controller']))

    def create_volumes(self, vdname, volconfs):
        vdisk = self.find_vdisk(vdname)
        if vdisk is None:
            raise MsaException('Vdisk "%s" not exist' % vdname)

        for conf in volconfs:
            self._create_volume(vdname, **conf)
            vdisk.add_volume(Volume(**conf))

    def restart(self, controllers=['sc', 'mc']):
        ''' restart controllers '''
        if 'sc' in controllers:
            self._run('restart sc both', True)
        if 'mc' in controllers:
            try:
                self._run('restart mc both', True)
            except Exception as e:
                print(e)

    def exit(self):
        self._send('exit')

    def __str__(self):
        self._scan()
        return '\n'.join(str(vdisk) for vdisk in self._vdisks)


if __name__ == '__main__':
    import sys

    if len(sys.argv) > 1:
        host = sys.argv[1]
    else:
        host = 'msa001'

    msa = MSA2000(host)
#    msa.delete_volumes('vd9', ['vd9_001'])
#    msa.create_volumes(
#        'vd9',
#        [{
#            'name': 'vd9_001',
#            'size': '599.5GB',
#            'access': 'rw',
#            'lun': '9',
#            'ports': 'a1,a2,b1,b2'
#        }], )
#
    print(msa)

    msa.exit()
