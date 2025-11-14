"""
Services to help local environment services and configurations.
"""

from typing import Optional

from pyspark.sql.session import SparkSession


class SparkSessionService:
    """
    A class for managing the Spark session.
    args:
        database_path (str): The path to the delta store.
    parameters:
        _existing_instance (Optional[SparkSessionService]): The existing instance of the SparkSessionService, if any.
        _database_path (str): The path to the delta store.
        _spark_session (SparkSession): The Spark session
    """

    _existing_instance: Optional["SparkSessionService"] = None
    _database_path: str
    _spark_session: SparkSession

    def __new__(cls, database_path: str) -> "SparkSessionService":
        if cls._existing_instance is None:
            cls._existing_instance = super(SparkSessionService, cls).__new__(cls)
            cls._existing_instance._database_path = database_path
            cls._existing_instance._spark_session = cls._existing_instance._create_spark_session()
        return cls._existing_instance

    @property
    def database_path(self) -> str:
        """
        The path to the delta store.
        """
        return self._database_path

    @database_path.setter
    def database_path(self, value: str) -> None:
        self._database_path = value

    @property
    def spark_session(self) -> SparkSession:
        """
        The Spark session.
        """
        return self._spark_session

    def _create_spark_session(self) -> SparkSession:
        """
        Creates a new Spark session, configured with the Delta Lake package.

        Returns:
            SparkSession: The Spark session.
        """
        builder = SparkSession.builder.config("spark.driver.memory", "2g")

        spark_session = builder.getOrCreate()
        return spark_session

    def get_spark_session(self) -> SparkSession:
        """
        Returns the Spark session.
        """
        return self.spark_session
