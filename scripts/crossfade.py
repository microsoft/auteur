import argparse

from node_schedule import create_mixer, create_local_playback_destination, start_node, connect, schedule_source, add_control_point, later, get_info

if __name__ == '__main__':
    parser = argparse.ArgumentParser('crossfade')

    parser.add_argument('source_uri')
    parser.add_argument('dest_uri')

    args = parser.parse_args()

    source_uri = args.source_uri
    dest_uri = args.dest_uri

    create_mixer('channel-1', config={
        'width': 1280,
        'height': 720,
        'sample-rate': 44100
    })
    create_local_playback_destination('centricular-output')
    start_node('centricular-output')
    connect('channel-1', 'centricular-output')
    start_node('channel-1')

    source_slot = schedule_source(source_uri, 'source_slot', 'channel-1', later(0), later(10),
            slot_config={
                'video::zorder': 2,
                'video::alpha': 1.0,
                'audio::volume': 1.0,
                'video::width': 1280,
                'video::height': 720,
                'video::sizing-policy': 'keep-aspect-ratio',
            })

    dest_slot = schedule_source(dest_uri, 'dest_slot', 'channel-1', later(5),
            slot_config={
                'video::zorder': 1,
                'video::alpha': 0.0,
                'audio::volume': 0.0,
                'video::width': 1280,
                'video::height': 720,
                'video::sizing-policy': 'keep-aspect-ratio',
            })

    add_control_point('control-1', source_slot, 'video::alpha', later(5), 1.0, interpolate=False)
    add_control_point('control-2', source_slot, 'video::alpha', later(10), 0.0)

    add_control_point('control-1', dest_slot, 'video::alpha', later(5), 0.0, interpolate=False)
    add_control_point('control-2', dest_slot, 'video::alpha', later(10), 1.0)

    add_control_point('control-1', source_slot, 'audio::volume', later(5), 1.0, interpolate=False)
    add_control_point('control-2', source_slot, 'audio::volume', later(10), 0.0)

    add_control_point('control-1', dest_slot, 'audio::volume', later(5), 0.0, interpolate=False)
    add_control_point('control-2', dest_slot, 'audio::volume', later(10), 1.0)

    get_info('channel-1')
