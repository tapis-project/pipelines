class BaseTapisPipelinesError(Exception):
    """
    Base Tapis Pipelines error class. All Error types should descend from this class.
    """
    def __init__(self, msg=None, code=400):
        """
        Create a new TapisError object.
        :param msg: (str) A helpful string
        :param code: (int) The HTTP return code that should be returned
        """
        self.msg = msg
        self.code = code


class ManifestFormatError(BaseTapisPipelinesError):
    """Error raised when manifest.json file does not pass schema validation."""
    pass


class PipelineConfigFormatError(BaseTapisPipelinesError):
    """Error raised when pipeline_config.json file does not pass schema validation."""
    pass


class PipelineConfigError(BaseTapisPipelinesError):
    """Error raised when pipeline_config.json file contains issues beyond schema validation ."""
    pass


class UnexpectedRuntimeError(BaseTapisPipelinesError):
    """
    Error raised when the pipeline code hits an unexpected error during runtime. These errors typically indicate
    additional error handling or other changes are needed to the pipelines code.
    """
    pass