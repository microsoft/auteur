import datetime
import subprocess
import re
import uuid
import os

import time

NOW = datetime.datetime.utcnow()
HERE = os.path.dirname(os.path.abspath(__file__))
EXE = os.path.join(HERE, 'target/debug/auteur-controller')
SERVER = 'ws://127.0.0.1:8080/ws/control'

def create_source(id_, uri):
    cmd = [EXE, SERVER, 'node', 'create', 'source', id_, uri]

    result = subprocess.check_output(cmd).decode().strip()

    print (result)

def create_rtmp_destination(id_, uri):
    cmd = [EXE, SERVER, 'node', 'create', 'destination', 'rtmp', id_, uri]

    result = subprocess.check_output(cmd).decode().strip()

    print (result)

def create_local_file_destination(id_, basename, max_size_time=None):
    cmd = [EXE, SERVER, 'node', 'create', 'destination', 'local-file', id_, basename]

    if max_size_time is not None:
        cmd += ['--max-size-time', str(max_size_time)]

    result = subprocess.check_output(cmd).decode().strip()

    print (result)

def create_mixer(id_, width, height, rate, fallback_image=None, fallback_timeout=None):
    cmd = [EXE, SERVER, 'node', 'create', 'mixer', id_, str(width), str(height), str(rate)]

    if fallback_image is not None:
        cmd += ['--fallback-image', fallback_image]

    if fallback_timeout is not None:
        cmd += ['--fallback-timeout', str(fallback_timeout)]

    result = subprocess.check_output(cmd).decode().strip()

    print (result)

def update_mixer(id_, width=None, height=None, rate=None):
    cmd = [EXE, SERVER, 'mixer', 'update', id_]

    if width is not None:
        cmd += ['--width', str(width)]

    if height is not None:
        cmd += ['--height', str(height)]

    if rate is not None:
        cmd += ['--sample-rate', str(rate)]

    result = subprocess.check_output(cmd).decode().strip()

    print (result)

def start_node(id_, cue_time=None, end_time=None):
    cmd = [EXE, SERVER, 'node', 'start', id_]

    if cue_time is not None:
        cmd += ['--cue-time', cue_time.isoformat() + 'Z']

    if end_time is not None:
        cmd += ['--end-time', end_time.isoformat() + 'Z']

    result = subprocess.check_output(cmd).decode().strip()
    print (result)

def connect(src_id, sink_id):
    link_id = '%s->%s_%s' % (src_id, sink_id, str(uuid.uuid4()))
    cmd = [EXE, SERVER, 'node', 'connect', link_id, src_id, sink_id]

    result = subprocess.check_output(cmd).decode().strip()

    print (result)

    return link_id

def disconnect(link_id):
    cmd = [EXE, SERVER, 'node', 'disconnect', link_id]

    result = subprocess.check_output(cmd).decode().strip()

    print (result)

def reschedule(id_, cue_time=None, end_time=None):
    cmd = [EXE, SERVER, 'node', 'reschedule', id_]

    if cue_time is not None:
        cmd += ['--cue-time', cue_time.isoformat() + 'Z']

    if end_time is not None:
        cmd += ['--end-time', end_time.isoformat() + 'Z']

    result = subprocess.check_output(cmd).decode().strip()
    print (result)

def remove_node(id_):
    cmd = [EXE, SERVER, 'node', 'remove', id_]

    result = subprocess.check_output(cmd).decode().strip()
    print (result)

def schedule_source(uri, src_id, dst_id, cue_time=None, end_time=None):
    create_source(src_id, uri)
    link_id = connect(src_id, dst_id)
    start_node(src_id, cue_time, end_time)
    return link_id

def set_mixer_slot_volume(id_, slot_id, volume):
    cmd = [EXE, SERVER, 'mixer', 'set-slot-volume', id_, slot_id, str(volume)]

    result = subprocess.check_output(cmd).decode().strip()

    print (result)

def later(delay):
    return NOW + datetime.timedelta(seconds=delay)

def get_info(id_=None):
    cmd = [EXE, SERVER, 'node', 'get-info']

    if id_ is not None:
        cmd += [id_]

    result = subprocess.check_output(cmd).decode().strip()
    print (result)

def add_control_point(controller_id, controllee_id, prop, time, value, interpolate=True):
    value = str(value)

    if interpolate:
        mode = 'interpolate'
    else:
        mode = 'set'

    cmd = [EXE, SERVER, 'node', 'add-control-point', controller_id, controllee_id, prop, time.isoformat() + 'Z', '--', value, mode]

    result = subprocess.check_output(cmd).decode().strip()
    print (result)

def remove_control_point(controller_id, controllee_id, prop):
    cmd = [EXE, SERVER, 'node', 'remove-control-point', controller_id, controllee_id, prop]

    result = subprocess.check_output(cmd).decode().strip()
    print (result)

if __name__ == '__main__':
    create_mixer('channel-1', 720, 480, 44100, fallback_image='/home/meh/Pictures/bark.jpg')
    create_rtmp_destination('centricular-output', 'rtmp://learntv-transcoder.eastus.azurecontainer.io/live/centricular-output')
    create_local_file_destination('local', os.path.join(HERE, 'capture'))
    start_node('centricular-output')
    start_node('local', later(5), later(20))
    connect('channel-1', 'centricular-output')
    connect('channel-1', 'local')
    start_node('channel-1')

    link_id = schedule_source('file:///home/meh/Videos/big_buck_bunny_720_stereo.mp4', 'bbb', 'channel-1', later(5))

    get_info('bbb')
