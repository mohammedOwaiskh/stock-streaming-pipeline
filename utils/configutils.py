import configparser

from pyspark import SparkConf


def read_application_config(
    key: str = None, section: str = "APP_DEFAULTS"
) -> str | list[tuple]:
    """Reads configuration from an application configuration file.

    Retrieves configuration values from a specified section of the application configuration file, with flexibility to fetch either a specific key or all items in a section.

    Args:
        key: Optional configuration key to retrieve. If None, returns all items in the section.
        section: Configuration section to read from, defaults to "APP_DEFAULTS".

    Returns:
        str or list[tuple]: Value of the specified key, or list of all key-value tuples in the section.

    Examples:
        >>> read_application_config('database_host')
        'localhost'
        >>> read_application_config(section='database')
        [('host', 'localhost'), ('port', '5432')]
    """
    config = configparser.ConfigParser()
    config.read("configurations\\applications.conf")
    return config.items(section) if key is None else config[section][key]


if __name__ == "__main__":
    print(read_application_config())
