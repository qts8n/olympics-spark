# Copyright Google Inc. 2018
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys
import time
import argparse

from google.cloud import pubsub
from google.cloud import storage
from ratelimit import rate_limited


def _parse_args():
    NOTE = '(if skipped or set to 0, the contents of the file will be published once)'

    parser = argparse.ArgumentParser(description='Publish lines from the .csv file to a pub/sub topic.')
    parser.add_argument('project'    , type=str, help='GCP project id')
    parser.add_argument('topic'      , type=str, help='GCP topic id')
    parser.add_argument('bucket'     , type=str, help='GCP storage bucket containing the file')
    parser.add_argument('filepath'   , type=str, help='path to the file within the bucket')
    parser.add_argument('time'       , type=float, nargs='?', default=0, help='running time of the generator in minutes ' + NOTE)
    parser.add_argument('rate'       , type=int  , nargs='?', default=0, help='line generation rate in lines per minute ' + NOTE)
    parser.add_argument('--no-header', action="store_true", help='if specified, line generation will start from row 1, otherwise from row 2')

    return parser.parse_args()


def read_file_from_bucket(path, bucket):
    client = storage.Client()
    bucket = client.get_bucket(bucket)
    blob = bucket.get_blob(path)
    return blob.download_as_string()


def generate(project, topic, file_contents, running_time, rate, header):
    publisher = pubsub.PublisherClient()
    topic_url = 'projects/' + project + '/topics/' + topic

    lines = file_contents.splitlines()
    if header: lines = lines[1:]
    total_lines = len(lines)

    num_lines_generated = 0
    def generate_line():
        nonlocal num_lines_generated
        line = lines[num_lines_generated % total_lines]
        publisher.publish(topic_url, line)
        num_lines_generated += 1

    now = start_time = time.time()
    if running_time != 0 and rate != 0:
        ONE_MINUTE = 60
        generate_line = rate_limited(rate, ONE_MINUTE)(generate_line)
        while now < start_time + running_time*ONE_MINUTE:
            generate_line()
            now = time.time()
    else:
        while num_lines_generated < total_lines:
            generate_line()

    elapsed_time = time.time() - start_time
    print("Elapsed time: %s minutes" % (elapsed_time / 60))
    print("Number of generated lines: %s" % num_lines_generated)


if __name__ == "__main__":
    args = _parse_args()
    generate(
        args.project,
        args.topic,
        read_file_from_bucket(args.filepath, args.bucket),
        args.time,
        args.rate,
        not args.no_header
    )