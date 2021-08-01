import argparse

from node_schedule import create_mixer, create_rtmp_destination, start_node, connect, schedule_source, add_control_point, later, get_info

if __name__ == '__main__':
    parser = argparse.ArgumentParser('web-overlay')

    parser.add_argument('source_uri')

    args = parser.parse_args()

    source_uri = args.source_uri

    create_mixer('channel-1', 1920, 1080, 44100)
    create_rtmp_destination('centricular-output', 'rtmp://learntv-transcoder.eastus.azurecontainer.io/live/centricular-output')
    start_node('centricular-output')
    connect('channel-1', 'centricular-output')
    start_node('channel-1')

    source_slot = schedule_source(source_uri, 'source_slot', 'channel-1', later(0))
    cef_slot = schedule_source('web://webkit.org/blog-files/3d-transforms/poster-circle.html', 'cef_slot', 'channel-1', later(0))

    add_control_point('control-1', source_slot, 'video::zorder', later(0), 1, interpolate=False)
    add_control_point('control-1', cef_slot, 'video::zorder', later(0), 2, interpolate=False)

    get_info('channel-1')
