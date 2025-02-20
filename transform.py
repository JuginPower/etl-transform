import json
import re
from pathlib import Path
import os
import logging


BASE_DIR = Path(__file__).resolve().parent
logger = logging.getLogger(__name__)
logging.basicConfig(filename="transform.log", encoding="utf-8", level=logging.WARNING, format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d, %H:%M:%S')

class FieldValidator:
    """
    The FieldValidator is validating whether all key-value pairs are complete in the incoming JSON data
    """
    def __init__(self):
        self.name = "FieldValidator"
        self.fields = ["title", "data", "datum", "category", "domain_name", "url_name"]

    def process_item(self, item: dict) -> dict:

        """
        Takes a dictionary item and test if all fields are present.
        Process it and write to

        :param item: A dictionary which contains the data to test.
        """

        drop = False

        for key in self.fields:
            if not item.get(key):
                drop = True
                logger.warning(f"Missing data for key {key} in item: {item}")

        if not self.is_valid_url(item.get("url_name")):
            drop = True
            logger.warning(f"url_name is not valid for item: {item}")

        if not drop:
            yield item

    def is_valid_url(self, url):
        """
        Test if the url is valid for the given standard pattern.

        :param url: The url to test
        :return: boolean for validation the url.
        """

        pattern = re.compile(r'^https?:\/\/(?!.*https)[^\s\/$.?#].[^\s]*$')
        return bool(pattern.match(url))


class TransformFloat:

    def __init__(self):
        self.name = "TransformFloat"

    def process_item(self, item: dict) -> dict:

        str_number = item.get("data")

        if not str_number:
            logger.warning(f"No data value for item: {item}")
        else:
            try:
                if len(str_number) > 7:
                    str_number = str_number.replace(".", "_").replace(",", ".")

                elif len(str_number) <= 7:
                    str_number = str_number.replace(",", ".")

                item["data"] = float(str_number)

            except ValueError:
                logger.warning(f"Could not convert data to float for item: {item}")

            yield item


class Pipeline:

    def __init__(self):

        pipes = [FieldValidator(), TransformFloat()]

    def process_item(self, item: dict):

        pass
