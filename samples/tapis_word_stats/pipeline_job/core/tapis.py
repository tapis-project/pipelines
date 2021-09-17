"""
Module for interacting with the Tapis API on behalf of a pipeline.
"""

from tapipy.tapis import Tapis
from config import

class TapisPipelineClient(object):
    """
    Class for managing Tapis interactions for a pipeline.
    """

    def __init__(self):
