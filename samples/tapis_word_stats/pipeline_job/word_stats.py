"""
Create word count statistics for a set of (text) input files.
"""


def get_stats_for_file(file_path):
    """
    Create some
    :param file_path:
    :return: dictionary of stats
    """


# the manifest file is passed as an argument --
manifest_file = ''

# parse the manifest file and get the list of files...
manifest = parse_manifest_file(manifest_file)
files = manifest.raw_files

# for each file, do some basic processing...
for file in files:
    stats = get_stats_for_file(file)
