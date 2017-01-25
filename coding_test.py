from pyspark import SparkContext
from pyspark.sql import SparkSession


class YelpDataProcessor(object):
    """
        Processes Yelp Data in JSON Format,
        1. in form of single json file
        2. in form of directory containing json files
        3. in form of compressed file containing one or more json files
    """

    def __init__(self, file_path):
        """
            Processes Yelp Data in JSON Format,
            1. in form of single json file
            2. in form of directory containing json files
            3. in form of compressed file containing one or more json files
        """
        self.spark_context = SparkContext("spark://localhost.localdomain:7077", "Coding Test App")
        self.spark = SparkSession \
            .builder \
            .appName("NewYorker Coding Test") \
            .config("spark.some.config.option", "some-value") \
            .getOrCreate()

        self.data_frame = self.spark.read.json(file_path)

    def query_user(self):
        """
            Query yelp_academic_dataset_user.json data
            lists all users with more than 4 star rating
        """
        pass

    def query_reviews(self):
        """
            Query yelp_academic_dataset_review.json data
            Number of positive, negative and neutral reviews. Criteria based on keyword:
            positive: good
            negative: bad
            neutral: all others
        """
        pass

    def query_business(self):
        """
            Query yelp_academic_dataset_business.json
            lists all business states by their average review count
        """
        pass

    def query_checkin(self):
        """
            Query yelp_academic_dataset_checkin.json data
            Get business_id with max number of checkins and the number of checkins
        """
        pass

    def query_tip(self):
        """
            Query yelp_academic_dataset_tip.json data
            Show number of tips per year
        """

    pass

if __name__ == '__main__':
    tar_file_path = "/home/training/Desktop/ny/ny_json.tar.gz"
    query = "business,tip,checkin,user,reviews"
    yelp_processor = YelpDataProcessor()

    if "business" in query:
        print("All business States by their average review count")
        yelp_processor.query_business()
    if "checkin" in query:
        print("Business with max number of check-ins")
        yelp_processor.query_checkin()

    if "user" in query:
        print("All users with more than 4 star rating")
        yelp_processor.query_user()
    if "reviews" in query:
        print("Number of positive, negative and neutral reviews")
        yelp_processor.query_reviews()
