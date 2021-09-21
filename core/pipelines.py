"""
Module for interacting with the Tapis API on behalf of a pipeline.
"""
import json
import os
import sys

from tapipy.tapis import Tapis
from config import parse_pipeline_config, parse_manifest_bytes
from errors import *
from meta import MetadataHelper

# all manifest files must have a name that begins with the following string; this is how the pipelines software
# recognizes manifest files from other kinds of input files:
TAPIS_PIPELINE_MANIFEST_FILENAME_PREFIX = "tapis_pipeline_manifest"

# the key used by the Meta helper class for an error
META_ERROR_STATUS_KEY = 'error'


class TapisPipelineClient(object):
    """
    Class for managing Tapis interactions for a pipeline.
    """

    def __init__(self):
        self.config_path = os.environ.get('TAPIS_PIPELINES_CONFIG_FILE_PATH', '/etc/tapis/pipeline_config.json')
        self.config = parse_pipeline_config(self.config_path)
        self.name = self.config.pipeline_name
        # parse the tapis_config
        try:
            self.tapis_base_url =  self.config.tapis_config['base_url']
        except KeyError:
            raise PipelineConfigError("tapis_config provided but tbase_url missing.")
        try:
            self.tapis_username =  self.config.tapis_config['username']
        except KeyError:
            raise PipelineConfigError("tapis_config provided but username missing.")
        # look for an access token in various places:
        try:
            self.access_token = self.config.tapis_config['access_token']
        except KeyError:
            try:
                self.access_token = os.environ['TAPIS_PIPELINES_ACCESS_TOKEN']
            except KeyError:
                try:
                    self.access_token = os.environ['_abaco_access_token']
                except KeyError:
                    self.access_token = None
        # if we didn't get an access token, look for a password:
        if not self.access_token:
            try:
                self.tapis_password = self.config.tapis_config['password']
            except Exception as e:
                raise PipelineConfigError("Could not find an Tapis access token or password. Exiting!")
        else:
            self.tapis_password = None
        if self.access_token:
            msg = f"Using the following config to instantiate the Tapis client. \n" \
                  f"base_url: {self.tapis_base_url} \n" \
                  f"username: {self.tapis_username} \n " \
                  f"access_token: {self.access_token[:5]}...{self.access_token[-5:]}"
        else:
            msg = f"Using the following config to instantiate the Tapis client. \n" \
                  f"base_url: {self.tapis_base_url} \n" \
                  f"username: {self.tapis_username} \n " \
                  f"password: {self.tapis_password[1]}..."
        print(msg)
        # instantiate the tapis client -----
        if self.access_token:
            try:
                self.tapis_client = Tapis(base_url=self.tapis_base_url,
                                          username=self.tapis_username,
                                          access_token=self.access_token)
            except Exception as e:
                raise PipelineConfigError(f"Failed to instantiate the tapis client using an access token. "
                                          f"Exception: {e}")
        else:
            try:
                self.tapis_client = Tapis(base_url=self.tapis_base_url,
                                          username=self.tapis_username,
                                          password=self.tapis_password)
            except Exception as e:
                raise PipelineConfigError(f"Failed to instantiate the tapis client using a password. "
                                          f"Exception: {e}")
        # set up the tapis metadata helper config ---
        # if the db name isn't provided, try to use "pipelines" as the db name..
        self._tapis_meta_db = self.config.tapis_config.get('meta_db', 'pipelines')
        # check to see if we have access to the db
        try:
            collections = self.tapis_client.meta.listCollectionNames(db=self._tapis_meta_db)
        except Exception as e:
            msg = f'Got exception trying to list collections on db: {self._tapis_meta_db}. ' \
                  f'Does user {self.tapis_username} have access to the db in the Meta API? Exception: {e}'
            print(msg)
            sys.exit(1)
        if type(collections) == bytes:
            collections = json.loads(collections)

        default_meta_collection = f'{self.tapis_username}.{self.name}'
        self._tapis_meta_collection = self.config.tapis_config.get('meta_collection', default_meta_collection)
        # if the collection does not already exist, try go create it:
        if self._tapis_meta_collection not in collections:
            try:
                self.tapis_client.meta.createCollection(db=self._tapis_meta_collection,
                                                        collection=self._tapis_meta_collection)
            except Exception as e:
                msg = f'Collection {self._tapis_meta_collection} did not exist and got error trying to created it. \n' \
                      f'Existing collections: {collections},\n' \
                      f'Exception: {e}'
                print(msg)
                sys.exit(1)
        # parse and check remote outbox
        self.remote_outbox = self.parse_remote_outbox_config()

    def get_meta_helper(self, job_name):
        """
        Helper method to instantiate a MetaHelper instance for a specific job.
        :param job_name:
        :return:
        """
        try:
            return MetadataHelper(tapis_client=self.tapis_client,
                                  db=self._tapis_meta_db,
                                  collection=self._tapis_meta_collection,
                                  job_name=job_name)
        except Exception as e:
            # TODO -- need
            msg = f'got exception trying to instantiate a MetadataHelper object for job: {job_name};\n' \
                  f'Exception: {e}'
            print(msg)
            raise UnexpectedRuntimeError(msg)

    def parse_remote_outbox_config(self):
        """
        Parses the remote outbox JSON config and creates a Box object with it.
        :return:
        """
        if self.config.remote_outbox['kind'] == 'tapis':
            return TapisSystemBox(system_id=self.config.remote_outbox['box_definition']['system_id'],
                                  path=self.config.remote_outbox['box_definition']['path'])
        else:
            raise NotImplementedError(f"Currently only support kind 'tapis' for remote_outbox configs. "
                                      f"Found: {self.config.remote_outbox['kind']}")

    def check_for_new_manifest_files(self):
        """
        Look for new manifest files in the remote outbox; if new manifest file found, claim it in metadata.
        :return:
        """
        if not self.remote_outbox.kind == 'tapis':
            raise NotImplementedError(f"Currently only support kind 'tapis' for remote_outbox configs. "
                                      f"Found: {self.config.remote_outbox['kind']}")

        self.check_tapis_system_for_new_manifest_files()

    def check_tapis_system_for_new_manifest_files(self):
        """
        Check for new manifest files on the tapis remote outbox system.
        :return: List of tapis file objects representing manifest files that are new since the last time
        the pipeline software ran. This function will create new metadata records for each file in the list.
        """
        try:
            file_list = self.tapis_client.files.listFiles(systemId=self.remote_outbox.system_id,
                                                          path=self.remote_outbox.path)
        except Exception as e:
            msg = f"Got exception from Tapis trying to list files on remote outbox. Will exit; e: {e}"
            print(msg)
            sys.exit(1)
        manifest_files = []
        for f in file_list:
            # manifest files must have a name that starts with
            if f.name.startswith(TAPIS_PIPELINE_MANIFEST_FILENAME_PREFIX):
                manifest_files.append(f)
        # check for manifest files that are not already claimed -- i.e., have an entry in metadata.
        new_manifest_files = []
        for f in manifest_files:
            job_id = self.get_job_id_from_manifest_name(f.name)
            m = MetadataHelper(tapis_client=self.tapis_client,
                               db=self._tapis_meta_db,
                               collection=self._tapis_meta_collection,
                               job_name=job_id)
            # the following method will return True if it creates a new meta record and false if there a
            # metadata entry already exists for this job, create it and add it to the
            if m.create():
                new_manifest_files.append(f)
        return new_manifest_files

    def get_job_id_from_manifest_name(self, file_name):
        """
        Computes the job_id from a manifest file name. This is just the last part of the name, after the
        manifest prefex.
        :param file_name: (string) The name of the file
        :return:
        """
        return file_name[len(TAPIS_PIPELINE_MANIFEST_FILENAME_PREFIX):]

    def validate_manifest(self, manifest_file):
        """
        Determines if a manifest file is valid: downloads the raw bytes, strips newlines and converts to JSON, and then
        validates against the manifest jsonschema.
        :param manifest_file: A tapis file object representing a manifest file.
        :return: bool -- True indicates the manifest file was valid, fale indicates it was invalid.
        """
        # parse manifest file and determine what input files are associated with the manifest; check that all
        # associated inputs files exist in the remote outbox.
        try:
            manifest_bytes = self.tapis_client.files.getContents(systemId=self.remote_outbox.system_id,
                                                                 path=manifest_file.path)
        except Exception as e:
            # TODO -- we should probably try a certain number of times and then eventually give up on this manifest..
            msg = f"Got exception trying to retrieve manifest file {manifest_file}; Exception: {e}"
            print(msg)
            m = self.get_meta_helper(self.get_job_id_from_manifest_name(manifest_file.name))
            m.update(statuskey=META_ERROR_STATUS_KEY, additional_info={"debug_data": msg})
            return False
        # parse and validate the manifest bytestream
        try:
            manifest = parse_manifest_bytes(manifest_bytes)
        except Exception as e:
            msg = f"Got exception trying to deserialize the manifest file {manifest_file}; Exception: {e}"
            print(msg)
            m = self.get_meta_helper(self.get_job_id_from_manifest_name(manifest_file.name))
            m.update(statuskey=META_ERROR_STATUS_KEY, additional_info={"debug_data": msg})
            return False
        # make sure every file listed in the manifest is on the remote system.
        for f in manifest['files']:
            path = f['file_path']
            try:
                self.tapis_client.files.listFiles(systemId=self.remote_outbox.system_id,
                                                  path=path)
            except Exception as e:
                # the file either doesn't exist or there was some other problem, so we cannot process this manifest
                # file.
                msg = f'Error checking file at path: {path} in manifest file: {manifest_file.path}; exception: {e}'
                print(msg)
                m = self.get_meta_helper(self.get_job_id_from_manifest_name(manifest_file.name))
                m.update(statuskey=META_ERROR_STATUS_KEY, additional_info={"debug_data": msg})
                return False
        return True

    def submit_job_for_manifest(self, manifest_file):
        """
        Submit a
        :param manifest_file:
        :return:
        """
        # when submitting a job to process manifest file and associated inputs.
        if not self.remote_outbox.kind == 'tapis':
            raise NotImplementedError(f"Currently only support kind 'tapis' for remote_outbox configs. "
                                      f"Found: {self.config.remote_outbox['kind']}")



    def check_for_completed_pipeline_jobs(self):
        """
        Reads the metadata for existing jobs in flight and checks with Tapis to determine if those jobs (i.e., actor
        executions or job executions) have completed.
        :return: a list of pipeline jobs that have just completed processing and need are ready for remote transfer.
        """
        completed_jobs = []
        # get the list of metadata jobs in status "processing_data"

        # check if any of the corresponding tapis jobs have completed
        return completed_jobs

    def copy_completed_job_outputs_to_remote_inbox(self, job):
        """
        The last step in a pipeline job life-cycle, this step copies the outputs from a recently completed job to
        the remote outbox.
        :param job:
        :return:
        """
        pass


class TapisSystemBox(object):
    """
    A class representing a local or remote inbox.outbox identified by a tapis system.
    """
    def __init__(self, system_id, path):
        self.kind = 'tapis'
        self.system_id = system_id
        self.path = path


def main():
    """
    Main program logic when executed from the command line.
    THis program is intended to run on a timer and possibly in response to new files being sent to the remote
    outbox or other events occurring in the Tapis framework.

    Until Tapis v3 has full support for notifications for events, run this program every 5 minutes to check for new
    actions that need to be taken with the pipeline.

    :return:
    """
    t = TapisPipelineClient()
    # step 1 -- look for new manifest files and submit new pipeline jobs
    new_manifest_files = t.check_for_new_manifest_files()
    # for each new manifest, check if it is valid, and if it is, submit a new job for it
    for f in new_manifest_files:
        if t.validate_manifest(f):
            t.submit_job_for_manifest(f)
    # step 2 -- check for completed pipeline jobs and update metadata accordingly
    completed_jobs = t.check_for_completed_pipeline_jobs()
    # step 3/4 -- for each completed job, copy the output files with the manifest to the remote inbox.
    for job in completed_jobs:
        t.copy_completed_job_outputs_to_remote_inbox(job)


if __name__ == '__main__':
    main()