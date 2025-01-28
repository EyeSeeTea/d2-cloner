class Config:
    """This `Config` class encapsulates the `api_version` to avoid modifying multiple methods. It can also be used to add more immutable configuration parameters. This class follows the Singleton pattern."""
    _instance = None

    def __new__(cls, pre_api_version=None, post_api_version=None):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
            cls._instance.pre_api_version = cls.convert_to_number(pre_api_version)
            cls._instance.post_api_version = cls.convert_to_number(post_api_version)
        return cls._instance

    @staticmethod
    def convert_to_number(version):
        """Convert the given string into a number, removing the '2.' prefix if present in the config file."""
        if isinstance(version, str):
            version = version.replace("2.", "")
            return int(version)
        return version

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            raise ValueError("Config has not been initialized. Call Config(...) first.")
        return cls._instance

    @classmethod
    def get_pre_api_version(cls):
        return cls.get_instance().pre_api_version

    @classmethod
    def get_post_api_version(cls):
        return cls.get_instance().post_api_version
