
import os
import subprocess
import sys

from resource_management import *

def read_pid(pidfile):
    try:
        with file(pidfile) as f:
            pid = f.read().strip()

        return int(pid)
    except IOError:  # file not exist
        return 0


def kill_process(pid):
    import signal
    try:
        Logger.info('Killing process %d' % pid)
        os.kill(pid, signal.SIGTERM)
    except OSError:
        pass


class Slave(Script):
    def install(self, env):
        self.install_packages(env)

    def configure(self, env):
        import params
       
        Logger.info('Configuring the service')

        Directory(params.base_dir, 
                  owner=params.user, group=params.group,
                  create_parents=True)
        Directory(format('{base_dir}/HDFS'),
                  owner=params.user, group=params.group)
        Directory(format('{base_dir}/HDFS/HDFScontrol'),
                  owner=params.user, group=params.group)
        Directory(format('{base_dir}/HDFS/HDFSlogs'),
                  owner=params.user, group=params.group)
        Directory(format('{base_dir}/HDFS/HDFSstaging'),
                  owner=params.user, group=params.group)
        Directory(format('{base_dir}/HDFS/HDFSprocessing'),
                  owner=params.user, group=params.group)
        Directory(format('{base_dir}/HDFS/HDFSqueue'),
                  owner=params.user, group=params.group)
        Directory(format('{base_dir}/HDFS/HDFSreaping'),
                  owner=params.user, group=params.group)

        Directory(params.app_root, 
                  owner=params.user, group=params.group,
                  create_parents=True)
        Directory(params.app_bin, 
                  owner=params.user, group=params.group)
        Directory(params.app_sbin,
                  owner=params.user, group=params.group,
                  create_parents=True)

        File(format('{app_sbin}/{app_start}'),
             mode=0755,
             owner=params.user, group=params.group,
             content=StaticFile(params.app_start))
        File(format('{app_bin}/{app_exec}'),
             mode=0755,
             owner=params.user, group=params.group,
             content=StaticFile(params.app_exec))

        Logger.info('Finished configuring the slave')

    def start(self, env):
        import params
        env.set_params(params)
        self.configure(env)
        pid_file = format('{base_dir}/{user}-{app}.pid')

        process_id_exists_command = as_sudo(['test', '-f', pid_file]) \
                           + ' && ' + as_sudo(['pgrep', '-F', pid_file])

        cmd = format('{app_sbin}/{app_start} {pid_file} {app_bin}/{app_exec} {base_dir} {concurrency} {throttle} {garbage_seconds}')
        daemon_cmd = as_user(cmd, params.user)

        File(pid_file, action='delete', not_if=process_id_exists_command)
        Execute(daemon_cmd, not_if=process_id_exists_command)

        Logger.info('Started unload service')

    def stop(self, env):
        import params
        env.set_params(params)

        pid_file = format('{base_dir}/{user}-{app}.pid')
        kill_process(read_pid(pid_file))

        Logger.info('Service stopped')

    def status(self, env):
        import status_params as params
        env.set_params(params)
        Logger.info('Status ...')

        check_process_status(params.pid_file)
 
    def install_package(self, env):
        print 'Installing slave', env


if __name__ == '__main__':
    Slave().execute()
