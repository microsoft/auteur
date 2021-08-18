import argparse

from node_schedule import create_mixer, create_rtmp_destination, start_node, connect, schedule_source, add_control_point, later, get_info

if __name__ == '__main__':
    parser = argparse.ArgumentParser('web-overlay')

    parser.add_argument('source_uri')

    args = parser.parse_args()

    source_uri = args.source_uri

    create_mixer('channel-1', config={
        'width': 1280,
        'height': 720,
        'sample-rate': 44100
    })
    create_rtmp_destination('centricular-output', 'rtmp://learntv-transcoder.eastus.azurecontainer.io/live/centricular-output')
    start_node('centricular-output')
    connect('channel-1', 'centricular-output')
    start_node('channel-1')

    source_slot = schedule_source(source_uri, 'source_slot', 'channel-1', later(0),
            slot_config={
                'video::zorder': 1,
                'video::width': 1280,
                'video::height': 720,
                'video::sizing-policy': 'keep-aspect-ratio',
            })
    cef_slot = schedule_source('web://webkit.org/blog-files/3d-transforms/poster-circle.html', 'cef_slot', 'channel-1', later(0),
            slot_config={
                'video::zorder': 2,
                'video::width': 1280,
                'video::height': 720,
                'video::sizing-policy': 'keep-aspect-ratio',
            })

    get_info('channel-1')
