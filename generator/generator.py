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

from google.cloud import pubsub
from google.cloud import storage
from ratelimit import rate_limited

from random import randrange

if len(sys.argv) != 7:
    print('Error: Incorrect number of parameters.')
    print('Usage: <project> <topic> <bucket> <csv-file> <running-time-min> <generation-rate-per-min>')
    sys.exit()

PROJECT    = sys.argv[1]
TOPIC      = sys.argv[2]
BUCKET     = sys.argv[3]
CSV_PATH   = sys.argv[4]
TOTAL_TIME = int(sys.argv[5])
RATE       = int(sys.argv[6])

ONE_MINUTE = 60

publisher = pubsub.PublisherClient()
topic_url = 'projects/{project_id}/topics/{topic}'.format(
    project_id=PROJECT,
    topic=TOPIC,
)

def read_csv():
    client = storage.Client()
    bucket = client.get_bucket(BUCKET)
    blob = bucket.get_blob(CSV_PATH)
    return blob.download_as_string()

CSV_LINES = read_csv().splitlines()
NUM_LINES = len(CSV_LINES)

num_lines_generated = 0
@rate_limited(RATE, ONE_MINUTE)
def generate_csv_line():
    global num_lines_generated
    line = CSV_LINES[randrange(NUM_LINES)]
    publisher.publish(topic_url, line)
    num_lines_generated += 1

now = start_time = time.time()
while now < start_time + TOTAL_TIME * ONE_MINUTE:
    generate_csv_line()
    now = time.time()

elapsed_time = time.time() - start_time
print("Elapsed time: %s minutes" % (elapsed_time / 60))
print("Number of generated lines: %s" % num_lines_generated)