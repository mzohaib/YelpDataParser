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
            .getOrCreate()

        self.data_frame = self.spark.read.json(file_path)

    def query_user(self):
        """
            Query yelp_academic_dataset_user.json data
            lists all users with more than 4 star rating
        """
        user_query_result = self.data_frame.select("user_id", "average_stars").where(
            self.data_frame["average_stars"] > 4)
        user_query_result.show()

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
        business_query_result = self.data_frame.groupBy("state").agg({"review_count": "avg"})
        business_query_result.show()

    def query_checkin(self):
        """
            Query yelp_academic_dataset_checkin.json data
            Show total number of checkins
        """
        checkin_query_result = self.data_frame.where(self.data_frame["type"] == "checkin")
        print(checkin_query_result.count())

    def query_tip(self):
        """
            Query yelp_academic_dataset_tip.json data
            Show number of tips per year
        """

    pass


if __name__ == '__main__':
    # TODO: Add command line arg support

    tar_file_path = "/home/training/Desktop/ny/ny_json.tar.gz"
    query = "business,user,checkin"
    yelp_processor = YelpDataProcessor(tar_file_path)

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
