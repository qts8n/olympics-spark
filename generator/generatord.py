import generatorf as fg

import pandas as pd
from io import BytesIO

import argparse


def _parse_args():
    NOTE = '(if skipped or set to 0, the contents of the file will be published once)'

    parser = argparse.ArgumentParser(description='Publish merged athlete events dataset to a pub/sub topic.')
    parser.add_argument('project'    , type=str, help='GCP project id')
    parser.add_argument('topic'      , type=str, help='GCP topic id')
    parser.add_argument('bucket'     , type=str, help='GCP storage bucket containing the file')
    parser.add_argument('directory'  , type=str, help='path to the directory containing 2 required files within the bucket')
    parser.add_argument('time'       , type=float, nargs='?', default=0, help='running time of the generator in minutes ' + NOTE)
    parser.add_argument('rate'       , type=int  , nargs='?', default=0, help='line generation rate in lines per minute ' + NOTE)
    parser.add_argument('--events'   , type=str, default="athlete_events.csv", help='name of the events file')
    parser.add_argument('--regions'  , type=str, default="noc_regions.csv", help='name of the regions file')

    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    directory = args.directory
    events_path = directory + '/' + args.events
    regions_path = directory + '/' + args.regions

    bucket = args.bucket
    events_file = fg.read_file_from_bucket(events_path, bucket)
    regions_file = fg.read_file_from_bucket(regions_path, bucket)

    events_dataset = pd.read_csv(BytesIO(events_file), dtype=str, na_filter=False)
    regions_dataset = pd.read_csv(BytesIO(regions_file), dtype=str, na_filter=False, header=0, names=['NOC', 'Region', 'Notes'])

    dataset = pd.merge(events_dataset, regions_dataset, on='NOC', how='left')
    fg.generate(
        args.project,
        args.topic,
        dataset.to_csv(index=False).encode('utf-8'),
        args.time,
        args.rate,
        False
    )