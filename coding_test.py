from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import year
from optparse import OptionParser


class YelpDataProcessor():
    """
        Processes Yelp Data in JSON Format,
        1. in form of single json file
        2. in form of directory containing json files
        3. in form of compressed tar.gz file containing one or more json files
    """

    def __init__(self, file_path):
        """
	  Initializes SparkSession and data frame object with the given path
        """
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
            self.data_frame["type"] == "user")
        user_query_result = user_query_result.where(self.data_frame["average_stars"] > 4)
        print("All users with more than 4 star rating")
        user_query_result.show()

    def query_reviews(self):
        """
            Query yelp_academic_dataset_review.json data
            Number of positive, negative and neutral reviews. Criteria based on keyword:
            positive: good
            negative: bad
            neutral: all others
        """
        review_query_result = self.data_frame.where(self.data_frame["type"] == "review")
        total_count = review_query_result.count()

        positive_count = review_query_result.where(self.data_frame["text"].like('%good%')).count()
        negative_count = review_query_result.where(self.data_frame["text"].like('%bad%')).count()

        neutral_count = total_count - (positive_count + negative_count)

        print("Number of positive, negative and neutral reviews")
        print('Positive:' + str(positive_count) + '\n' + 'Negative:' + str(negative_count) + '\n' + 'Neutral:' + str(
            neutral_count))

    def query_business(self):
        """
            Query yelp_academic_dataset_business.json
            lists all business states by their average review count
        """
        business_query_result = self.data_frame.where(self.data_frame["type"] == "business") \
            .groupBy("state").agg({"review_count": "avg"})

        print("All business States by their average review count")
        business_query_result.show()

    def query_checkin(self):
        """
            Query yelp_academic_dataset_checkin.json data
            Show total number of checkins
        """
        checkin_query_result = self.data_frame.where(self.data_frame["type"] == "checkin")
        print("Business with max number of check-ins")
        print("checkins:" + str(checkin_query_result.count()))

    def query_tip(self):
        """
            Query yelp_academic_dataset_tip.json data
            Show number of tips per year
        """
        tip_query_result = self.data_frame.where(self.data_frame["type"] == "tip") \
            .groupBy(year(self.data_frame["date"]).alias("year")) \
            .agg({"*": "count"})
        print("Number of tips per year")
        tip_query_result.show()


if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-q', '--query', dest="query", help="comma seperated query types for e.g tip,user")
    parser.add_option('-f', '--filepath', dest="filepath", help="tar file path")
    options, args = parser.parse_args()

    if options.query and options.filepath:
        yelp_processor = YelpDataProcessor(options.filepath)

        if "business" in options.query:
            yelp_processor.query_business()
        if "checkin" in options.query:
            yelp_processor.query_checkin()
        if "user" in options.query:
            yelp_processor.query_user()
        if "reviews" in options.query:
            yelp_processor.query_reviews()
        if "tip" in options.query:
            yelp_processor.query_tip()
    else:
        print("Error: Required parameters missing, please run the script in the following way: "
              "coding_test.py -f /path/to/file.tar.gz -q user,tip,reviews,business,checkin")
