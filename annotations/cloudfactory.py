from parquet_write_automation.cloud.scripts.main.cloud import Cloud
from parquet_write_automation.cloud.scripts.main.gcs import Gcs


class CloudFactory:
    """
    Factory class for all class which implements Cloud
    all class name(in lower case) and class should be added
    in __all_storage dictionary in order to get there factory method
    """
    _all_storage = {
        "gcs": Gcs,
        "abstract": Cloud

    }

    @classmethod
    def get_cloud_storage(cls, cloud_name: str):
        """
        Factory method to get cloud storage instance based on
        registered cloud name
        :param cloud_name: name of cloud storage
        :return: instance
        """
        if not isinstance(cloud_name, str):
            raise TypeError("cloud_name should be of string type")

        key = cloud_name.lower()
        try:
            return cls._all_storage[key]()
        except KeyError:
            keys = cls.get_all_register_class_name()
            raise KeyError("Invalid cloud name supplied\n"
                           "All implemented cloud storage are {}".format(keys))

    @classmethod
    def get_all_register_class_name(cls)->list:
        """
        this function returns list of all class name
        register with factory
        :return: list(str)
        """
        return list(cls._all_storage.keys())

    @classmethod
    def get_all_registered_class(cls)->list:
        """
        this functions returns all register class
        :return: list[class]
        """
        return list(cls._all_storage.values())
