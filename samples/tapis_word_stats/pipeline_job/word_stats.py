"""
Create word count statistics for a set of (text) input files.

When testing locally, be sure to
$ export _tapisJobUUID=12345.out
before running the program.

"""
import os
JOB_ID = os.environ.get('_tapisJobUUID')
print(f"top of word_stats.py for JOB_ID: {JOB_ID}...")

from core.config import parse_manifest_file


INPUT_DATA_CONTAINER_DIR = '/TapisInput'
MANIFEST_FILE_PATH = os.environ.get('TAPIS_MANIFEST_FILE_PATH', '/TapisInput/manifest.json') or '/TapisInput/manifest.json'
if MANIFEST_FILE_PATH == 'null':
    MANIFEST_FILE_PATH = '/TapisInput/manifest.json'
OUTPUT_DATA_CONTAINER_DIR = '/TapisOutput'

print(f"paths being used: \n"
      f"MANIFEST_FILE_PATH: {MANIFEST_FILE_PATH} \n"
      f"INPUT_DATA_CONTAINER_DIR: {INPUT_DATA_CONTAINER_DIR} \n"
      f"OUTPUT_DATA_CONTAINER_DIR: {OUTPUT_DATA_CONTAINER_DIR} \n")

def get_stats_for_file(file_path):
    """
    Create some
    :param file_path:
    :return: dictionary of stats
    """
    print(f"top of get_stats_for_file for {file_path}")
    with open(file_path, 'r') as f:
        text = f.read()
    return {'word_count': len(text.split())}


# parse the manifest file and get the list of files
try:
    manifest = parse_manifest_file(MANIFEST_FILE_PATH)
    files = manifest.files
except Exception as e:
    msg = f'Got exception trying to parse the manifest file and get the files attribute. Exception: {e}'
    print(msg)
    raise e


# final stats result object; will be printed out to a file at the end
stats = {}

# for each file, do some basic processing...
for f in files:
    print(f"processing file: {f}")
    full_input_path = os.path.join(INPUT_DATA_CONTAINER_DIR, f['file_path'])
    stats[f['file_path']] = get_stats_for_file(full_input_path)

# write out results --
full_output_path = os.path.join(OUTPUT_DATA_CONTAINER_DIR, f'{JOB_ID}.out')
with open(full_output_path, 'w') as f:
    for k, v in stats.items():
        words = str(v['word_count'])
        f.write(f'{k}\n******\n ')
        f.write(f'words: {words}\n\n')

