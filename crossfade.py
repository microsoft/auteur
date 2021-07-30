import argparse

from node_schedule import create_mixer, create_rtmp_destination, start_node, connect, schedule_source, add_control_point, later, get_info

if __name__ == '__main__':
    parser = argparse.ArgumentParser('crossfade')

    parser.add_argument('source_uri')
    parser.add_argument('dest_uri')

    args = parser.parse_args()

    source_uri = args.source_uri
    dest_uri = args.dest_uri

    create_mixer('channel-1', 1920, 1080, 44100)
    create_rtmp_destination('centricular-output', 'rtmp://learntv-transcoder.eastus.azurecontainer.io/live/centricular-output')
    start_node('centricular-output')
    connect('channel-1', 'centricular-output')
    start_node('channel-1')

    source_slot = schedule_source(source_uri, 'source_slot', 'channel-1', later(0), later(10))
    dest_slot = schedule_source(dest_uri, 'dest_slot', 'channel-1', later(5))

    add_control_point('control-1', source_slot, 'video::zorder', later(0), 2, interpolate=False)
    add_control_point('control-1', dest_slot, 'video::zorder', later(0), 1, interpolate=False)

    add_control_point('control-1', source_slot, 'video::alpha', later(5), 1.0, interpolate=False)
    add_control_point('control-2', source_slot, 'video::alpha', later(10), 0.0)

    add_control_point('control-1', source_slot, 'audio::volume', later(5), 1.0, interpolate=False)
    add_control_point('control-2', source_slot, 'audio::volume', later(10), 0.0)

    add_control_point('control-1', dest_slot, 'audio::volume', later(5), 0.0, interpolate=False)
    add_control_point('control-2', dest_slot, 'audio::volume', later(10), 1.0)

    get_info('channel-1')
