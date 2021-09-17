import json
import logging
from datetime import datetime


class MetadataHelper:

    def __init__(self, tapis_client, db, collection, job_name):
        self.db = db
        self.collection = collection
        self.job_name = job_name
        self.tapis_client = tapis_client
        self.STATUS = {
            'init': 'Created metadata',
            'transfer_to_local': 'Started data transfer to LOCAL',
            'transfer_to_local_done': 'Finished data transfer to LOCAL',
            'unpack_data_on_local': 'Started data unpack on LOCAL',
            'unpack_data_on_local_done': 'Finished data unpack on LOCAL',
            'processing_data': 'Started processing data',
            'processing_data_done': 'Finished processing data',
            'pack_output': 'Started packaging of processed data on LOCAL',
            'pack_output_done': 'Finished packaging of processed data on LOCAL',
            'transfer_to_remote': 'Start data transfer to REMOTE',
            'transfer_to_remote_done': 'Finished data transfer REMOTE',
            'pipeline_done': 'Finished pipeline',
            'error': 'Error',
            'other': 'Other',
            'test': 'Test'
        }
        self.logger = logging.getLogger('MetadataHelper')
        self.logger.debug('Created instance for {}'.format(self.job_name))

    def create(self):
        '''
        if metadata does not exist for this job_name, create it
        job name is likely the input file, e.g. something_12345678_req123.tar
        '''

        if self.get():
            self.logger.info('Metadata record already exists for {}, not creating another.'.format(self.job_name))
            return False
        else:
            self.tapis_client.meta.createDocument(
                db=self.db,
                collection=self.collection,
                request_body={
                    'name': self.job_name,
                    'current_status': {
                        'datetime': datetime.now().strftime("%m/%d/%Y, %H:%M:%S"),
                        'status': self.STATUS['init'],
                        'additional_info':{}
                    },
                    "status_history":[]
                }
            )
            self.logger.info('Created metadata record for {}.'.format(self.job_name))
            return True



    def get(self):

        '''
        Get metadata for this job_name.
        Returns None if Tapis call does not succeed.
        '''

        try:
            metadata = json.loads(self.tapis_client.meta.listDocuments(
                db=self.db,
                collection=self.collection,
                filter=str({'name':self.job_name})
            ))[0]
            return metadata
        except Exception as e:
            print(e)
            return None

    def print(self):
        metadata = self.get()
        print(json.dumps(metadata, indent=2))


    def update(self, statuskey, additional_info={}):

        metadata = self.get()

        metadata['status_history'].append(metadata['current_status'])

        new_status = {
            'datetime': datetime.now().strftime("%m/%d/%Y, %H:%M:%S"),
            'status': self.STATUS[statuskey],
            'status_description': self.STATUS[statuskey],
            'additional_info': additional_info,
        }

        #print(new_status)
        #print(metadata['status_history'])

        # TODO: add new field status_description, but might have to do that in several places like the dashboard too
        try:
            self.tapis_client.meta.modifyDocument(
                db=self.db,
                collection=self.collection,
                docId=metadata['_id']['$oid'],
                request_body={
                    'current_status':new_status,
                    'status_history':metadata['status_history']
                }
            )
        except Exception as e:
            print(e)

