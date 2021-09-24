"""
Module for interacting with the Tapis API on behalf of a pipeline.
"""
import json
import os
import sys

from tapipy.tapis import Tapis
from core.config import parse_pipeline_config, parse_manifest_bytes
from core import errors
from core.meta import MetadataHelper

# all manifest files must have a name that begins with the following string; this is how the pipelines software
# recognizes manifest files from other kinds of input files:
TAPIS_PIPELINE_MANIFEST_FILENAME_PREFIX = "tapis_pipeline_manifest_"

# the key used by the Meta helper class for an error
META_ERROR_STATUS_KEY = 'ERROR'

# terminal job states for tapis jobs -- TODO
TERMINAL_JOB_STATES = ['FAILED', 'FINISHED']


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
            raise errors.PipelineConfigError("tapis_config provided but tbase_url missing.")
        try:
            self.tapis_username =  self.config.tapis_config['username']
        except KeyError:
            raise errors.PipelineConfigError("tapis_config provided but username missing.")
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
                raise errors.PipelineConfigError("Could not find an Tapis access token or password. Exiting!")
        else:
            self.tapis_password = None
        if self.access_token:
            msg = f"Using the following config to instantiate the Tapis client. \n" \
                  f"base_url: {self.tapis_base_url} \n" \
                  f"username: {self.tapis_username} \n" \
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
                raise errors.PipelineConfigFormatError(f"Failed to instantiate the tapis client using an access token. "
                                                       f"Exception: {e}")
        else:
            try:
                self.tapis_client = Tapis(base_url=self.tapis_base_url,
                                          username=self.tapis_username,
                                          password=self.tapis_password)
                self.tapis_client.get_tokens()
            except Exception as e:
                raise errors.PipelineConfigFormatError(f"Failed to instantiate the tapis client using a password. "
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
                self.tapis_client.meta.createCollection(db=self._tapis_meta_db,
                                                        collection=self._tapis_meta_collection)
            except Exception as e:
                msg = f'Collection {self._tapis_meta_collection} did not exist and got error trying to created it. \n' \
                      f'Existing collections: {collections} in db: {self._tapis_meta_db},\n' \
                      f'Exception: {e}\n'
                print(msg)
                try:
                      msg = f'Extra debug info:\n' \
                            f'Request: {e.request.url}; {e.request.method}; body: {e.request.body}; ' \
                            f'Response: {e.response.content}'
                      print(msg)
                except Exception as e:
                    print(f"Couldn't print extra debug info; exception: {e}")
                sys.exit(1)
        # parse and check remote outbox ---
        self.remote_outbox = self.parse_remote_outbox_config()
        # check and parse the pipeline job
        self.pipeline_job = self.parse_pipeline_job_config()

    def get_meta_helper(self, remote_id):
        """
        Helper method to instantiate a MetaHelper instance for a specific job.
        :param remote_id: This is the id of the manifest file, as determined from the file name (that is, as determined
        by the remote system). We have to use these id's for the metadata because it is possible we will not be able
        to even validate a given manifest (we may not be able to even load its contents).
        :return:
        """
        try:
            return MetadataHelper(tapis_client=self.tapis_client,
                                  db=self._tapis_meta_db,
                                  collection=self._tapis_meta_collection,
                                  job_name=remote_id)
        except Exception as e:
            # TODO -- need
            msg = f'got exception trying to instantiate a MetadataHelper object for remote_id: {remote_id};\n' \
                  f'Exception: {e}'
            print(msg)
            raise errors.UnexpectedRuntimeError(msg)

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

    def parse_pipeline_job_config(self):
        """
        Parses the pipelin_job config and returns a pipeline job object.
        :return:
        """
        # tapis app job type ---
        if 'tapis_app_job' in self.config.pipeline_job.keys():
            app = TapisPipelineApp(app_id=self.config.pipeline_job['tapis_app_job']['app_id'],
                                   app_version=self.config.pipeline_job['tapis_app_job']['app_version'],
                                   manifest_input_name=self.config.pipeline_job['tapis_app_job']['manifest_input_name']
                                   )
            # check for access to the version of the tapis app
            try:
                tapis_app = self.tapis_client.apps.getApp(appId=app.app_id, appVersion=app.app_version)
            except Exception as e:
                msg = f"Got exception trying to check access to Tapis app with id: {app.app_id}; " \
                      f"version: {app.app_version}\n"
                print(msg)
                try:
                    msg = f"Additional debug data:\n" \
                            f'Request: {e.request.url}; {e.request.method}; body: {e.request.body}; ' \
                            f'Response: {e.response.content}'
                    print(msg)
                except Exception as e:
                    print(f"Couldn't print extra debug info; exception: {e}")
                sys.exit(1)
            # check that this version of the app has the manifest input
            for inp in tapis_app.jobAttributes.fileInputs:
                # TODO -- this will need to be updated as soon as the app definition is updated to move the input
                # name out of the meta section and into a top-level attribute
                if inp.meta.name == app.manifest_input_name:
                    break
            else:
                msg = f"Did not find an input with mame {app.manifest_input_name}. Found the following inputs:\n" \
                      f"{tapis_app.jobAttributes.fileInputs}.\n" \
                      f"Double-check your pipeline config. Exiting..."
                print(msg)
                raise errors.PipelineConfigError(msg)
            return app
        elif 'tapis_actor_job' in self.config.pipeline_job.keys():
            raise NotImplementedError(f"Currently do not support tapis_actor_job type.")
        else:
            msg = "Did not find a tapis_app_job or tapis_actor_job pipeline in the config. Exiting..."
            print(msg)
            raise errors.PipelineConfigFormatError(msg)

    def check_for_new_manifest_files(self):
        """
        Look for new manifest files in the remote outbox; if new manifest file found, claim it in metadata.
        :return:
        """
        if not self.remote_outbox.kind == 'tapis':
            raise NotImplementedError(f"Currently only support kind 'tapis' for remote_outbox configs. "
                                      f"Found: {self.config.remote_outbox['kind']}")

        return self.check_tapis_system_for_new_manifest_files()

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
            job_id = self.get_remote_id_from_manifest_name(f.name)
            m = MetadataHelper(tapis_client=self.tapis_client,
                               db=self._tapis_meta_db,
                               collection=self._tapis_meta_collection,
                               job_name=job_id)
            # the following method will return True if it creates a new meta record and false if there a
            # metadata entry already exists for this job, create it and add it to the
            if m.create():
                new_manifest_files.append(f)
        return new_manifest_files

    def get_remote_id_from_manifest_name(self, file_name):
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
            m = self.get_meta_helper(self.get_remote_id_from_manifest_name(manifest_file.name))
            m.update(statuskey=META_ERROR_STATUS_KEY, additional_info={"debug_data": msg})
            return False
        # parse and validate the manifest bytestream
        try:
            manifest = parse_manifest_bytes(manifest_bytes)
        except Exception as e:
            msg = f"Got exception trying to deserialize the manifest file {manifest_file}; Exception: {e}"
            print(msg)
            m = self.get_meta_helper(self.get_remote_id_from_manifest_name(manifest_file.name))
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
                m = self.get_meta_helper(self.get_remote_id_from_manifest_name(manifest_file.name))
                m.update(statuskey=META_ERROR_STATUS_KEY, additional_info={"debug_data": msg})
                return False
        # create an honest Manifest object
        return Manifest(pipeline_name=self.name,
                        file_path=manifest_file.path,
                        remote_id=self.get_remote_id_from_manifest_name(manifest_file.name),
                        tapis_url=manifest_file.uri,
                        inputs=manifest.files)

    def get_tapis_job_dict_for_manifest(self, manifest):
        """
        Create a job object from an instance of a Manifest that can be used to submit a tapis job.
        :param manifest:
        :return:
        """
        job = {
            "name": manifest.tapis_job_name,
            "appId": self.pipeline_job.app_id,
            "appVersion": self.pipeline_job.app_version,
            # we first add the manifest input
            "fileInputs": [{
                # "sourceUrl": manifest.tapis_url,
                "sourceUrl": f"tapis://{self.remote_outbox.system_id}/{manifest.file_path}",
                "meta": {
                    "name": self.pipeline_job.manifest_input_name,
                    "required": True
                }
            }]
        }
        # now add additional inputs --
        for inp in manifest.inputs:
            inp_path = inp['file_path']
            job['fileInputs'].append({
                # "sourceUrl": f"tapis://{self.tapis_client.tenant_id}/{self.remote_outbox.system_id}/{inp_path}",
                "sourceUrl": f"tapis://{self.remote_outbox.system_id}/{inp_path}",
                "targetPath": inp_path
            })
        return job

    def submit_job_for_manifest(self, manifest):
        """
        Submit a new pipeline job for a manifest object.
        :param manifest: An instance of a Manifest; e.g., as generated from a call to validate_manifest().
        :return:
        """
        # when submitting a job to process manifest file and associated inputs.
        if not self.remote_outbox.kind == 'tapis':
            raise NotImplementedError(f"Currently only support kind 'tapis' for remote_outbox configs. "
                                      f"Found: {self.config.remote_outbox['kind']}")
        if not self.pipeline_job.kind == 'tapis_app':
            raise NotImplementedError(f"Currently only support kind 'tapis_app' for pipeline jobs. "
                                      f"Found: {self.pipeline_job.kind}")
        job = self.get_tapis_job_dict_for_manifest(manifest)
        print(f"submitting job: {job}")
        try:
            job_response = self.tapis_client.jobs.submitJob(**job)
        except Exception as e:
            msg = f"Got exception trying to submit Tapis job for pipeline job manifest: {manifest}; e: {e}\n"
            print(msg)
            try:
                msg = f"Additional debug data:\n" \
                      f'Request: {e.request.url}; {e.request.method}; body: {e.request.body}; ' \
                      f'Response: {e.response.content}'
                print(msg)
            except Exception as e:
                print(f"Couldn't print extra debug info; exception: {e}")
                # TODO -- if we could not submit the job, we need to set the manifest to an error state;
                # however it might be best to try the job a few times before setting it to error.
                m = self.get_meta_helper(remote_id=manifest.remote_id)
                m.update(statuskey=META_ERROR_STATUS_KEY, additional_info={"debug_data": msg})
                return None
        # update the metadata with the new tapis job --
        m = self.get_meta_helper(remote_id=manifest.remote_id)
        info = {"kind": "tapis_job",
                "tapis_job_uuid": job_response.uuid,
                "tapis_job_status": job_response.status}
        m.update(statuskey='JOB_SUBMITTED_TO_TAPIS', additional_info=info)

    def get_all_remote_job_ids(self, statuses=[]):
        """
        Helper method to read the metadata and get all remote job id's with status in a list of specified statuses.
        :param status: A list of statuses to filter all jobs by.
        :return:
        """
        result = []
        for s in statuses:
            try:
                result.extend(json.loads(self.tapis_client.meta.listDocuments(db=self._tapis_meta_db,
                                                                              collection=self._tapis_meta_collection,
                                                                              filter=str({'status': s}))))
            except Exception as e:
                msg = f"Got exception trying to query Tapis job for jobs in status: {s}; e: {e}\n"
                print(msg)
                try:
                    msg = f"Additional debug data:\n" \
                          f'Request: {e.request.url}; {e.request.method}; body: {e.request.body}; ' \
                          f'Response: {e.response.content}'
                    print(msg)
                except Exception as e:
                    print(f"Couldn't print extra debug info; exception: {e}")
                # TODO -- need to fail something in this case...
        return result

    def check_for_completed_pipeline_jobs(self):
        """
        Reads the metadata for existing jobs in flight and checks with Tapis to determine if those jobs (i.e., actor
        executions or job executions) have completed.
        :return: a list of pipeline jobs that have just completed processing and need are ready for remote transfer.
        """
        completed_jobs = []
        # get the list of metadata jobs in status "processing_data"
        jobs = self.get_all_remote_job_ids(statuses=["JOB_SUBMITTED_TO_TAPIS"])
        for job in jobs:
            # get job uuid
            job_uuid = job['additional_info']['tapis_job_uuid']
            # check if any of the corresponding tapis jobs have completed
            try:
                tapis_job = self.tapis_client.jobs.getJob(jobUuid=job_uuid)
            except Exception as e:
                msg = f"Got exception trying to look up job in Tapis for job: {job_uuid}; e: {e}\n"
                print(msg)
                try:
                    msg = f"Additional debug data:\n" \
                          f'Request: {e.request.url}; {e.request.method}; body: {e.request.body}; ' \
                          f'Response: {e.response.content}'
                    print(msg)
                except Exception as e:
                    print(f"Couldn't print extra debug info; exception: {e}")
                # TODO -- do we need to fail the job??
                continue
            if tapis_job.status in TERMINAL_JOB_STATES:
                m = self.get_meta_helper(remote_id=job['name'])
                m.update(statuskey=tapis_job.status)
                completed_jobs.append(tapis_job)
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


class TapisPipelineApp(object):
    """
    Class representing a Tapis app serving as the pipeline job
    """
    def __init__(self, app_id, app_version, manifest_input_name):
        self.kind = 'tapis_app'
        self.app_id = app_id
        self.app_version = app_version
        self.manifest_input_name = manifest_input_name


class Manifest(object):
    """
    Class representing a manifest object.
    """
    def __init__(self, pipeline_name, file_path, remote_id, tapis_url, inputs):
        self.kind = 'tapis_file'
        self.pipeline_name = pipeline_name
        self.remote_id = remote_id
        self.file_path = file_path
        self.tapis_job_name = f"{pipeline_name}.{remote_id}"
        # tapis jobs can have a manimum name length of 64 chars so if the computed name is too long, we do some
        # heuristics to determine how best to proceed.
        if len(self.tapis_job_name) > 64:
            # if the remote_id is "long", assume we are safe to use that:
            if len(remote_id) > 15:
                self.tapis_job_name = remote_id
            # otherwise, the pipeline name must be very long; we can use the first set of characters from the
            # pipeline name:
            # use 63-len() instead of 64-len() to account for the extra "." character between the fragments.
            pipeline_name_fragment = pipeline_name[0:63-len(remote_id)]
            self.tapis_job_name = f"{pipeline_name_fragment}.{remote_id}"
        self.tapis_url = tapis_url
        self.inputs = inputs


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
        manifest = t.validate_manifest(f)
        if manifest:
            t.submit_job_for_manifest(manifest)
    # step 2 -- check for completed pipeline jobs and update metadata accordingly
    completed_jobs = t.check_for_completed_pipeline_jobs()
    # step 3/4 -- for each completed job, copy the output files with the manifest to the remote inbox.
    for job in completed_jobs:
        t.copy_completed_job_outputs_to_remote_inbox(job)


if __name__ == '__main__':
    main()