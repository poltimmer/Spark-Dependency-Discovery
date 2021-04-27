from pyspark.sql import SparkSession

def main(): 

    #spark init code
    spark:SparkSession = SparkSession.builder \
      .master("local[12]")   \
      .appName("SparkDB")   \
      .getOrCreate()    
    
    rdd = spark.sparkContext.textFile("./data/VR_20051125.txt")
    comma_rdd = rdd.map(lambda row: tab_to_comma(row))

    print("initial partition count:"+str(rdd.getNumPartitions()))
    
    # analysis part
    analyse(spark, comma_rdd)
    spark.stop()

# Convert tab delimited row to comma delimited and strip all spacing
def tab_to_comma(row):
    arr = row.split("\t")
    result = arr[0]
    for i in range(1, len(arr)):
        result += "," + arr[i].strip(" ")
    return result

def analyse(spark, file):
    header = file.first().split(",")  # Store header as array
    

    # # colnames can be used together with attr_idx in order to index based on column name. Redundant for now, but can be useful
    # colnames = ["snapshot_dt", "county_id", "county_desc", "voter_reg_num", "ncid", "status_cd", "voter_status_desc", "reason_cd", "voter_status_reason_desc", "absent_ind", "name_prefx_cd", "last_name", "first_name", "midl_name", "name_sufx_cd", "house_num", "half_code", "street_dir", "street_name", "street_type_cd", "street_sufx_cd", "unit_designator", "unit_num", "res_city_desc", "state_cd", "zip_code", "mail_addr1", "mail_addr2", "mail_addr3", "mail_addr4", "mail_city", "mail_state", "mail_zipcode", "area_cd", "phone_num", "race_code", "race_desc", "ethnic_code", "ethnic_desc", "party_cd", "party_desc", "sex_code", "sex", "age", "birth_place", "registr_dt", "precinct_abbrv", "precinct_desc", "municipality_abbrv", "municipality_desc", "ward_abbrv", "ward_desc", "cong_dist_abbrv", "cong_dist_desc", "super_court_abbrv", "super_court_desc", "judic_dist_abbrv", "judic_dist_desc", "NC_senate_abbrv", "NC_senate_desc", "NC_house_abbrv", "NC_house_desc", "county_commiss_abbrv", "county_commiss_desc", "township_abbrv", "township_desc", "school_dist_abbrv", "school_dist_desc", "fire_dist_abbrv", "fire_dist_desc", "water_dist_abbrv", "water_dist_desc", "sewer_dist_abbrv", "sewer_dist_desc", "sanit_dist_abbrv", "sanit_dist_desc", "rescue_dist_abbrv", "rescue_dist_desc", "munic_dist_abbrv", "munic_dist_desc", "dist_1_abbrv", "dist_1_desc", "dist_2_abbrv", "dist_2_desc", "confidential_ind", "cancellation_dt", "vtd_abbrv", "vtd_desc", "load_dt", "age_group"]
    # attr_idx = {name:i for i, name in enumerate(colnames)}

    # Create seperate and combined rdd per attribute
    rdd_left = file.map(lambda x: x.split(",")[1]) # county_id
    rdd_right = file.map(lambda x: x.split(",")[2]) # county_desc
    rdd_combined = file.map(lambda x: x.split(",")[1:3]) # both

    #count the amount of times a certain word occurs in the file
    left_word_count = rdd_left \
                    .map(lambda row: (row, 1)) \
                    .reduceByKey(lambda a, b: a + b) \
                    .collect()
                    
    right_word_count = rdd_right \
                    .map(lambda row: (row, 1)) \
                    .reduceByKey(lambda a, b: a + b) \
                    .collect()

    # Sort on number of occurrences
    left_sorted = sorted(left_word_count, key=lambda x: x[1])
    right_sorted = sorted(right_word_count, key=lambda x: x[1])


    # TODO if not all 'true'/identical for sorted array of values:counts for the column pairs, break out, as this next step won't make sense
    attribute_mapping = {left[0]: right[0] for left, right in zip(left_sorted, right_sorted)} 


    # Mapping array is of format [("1", "Arizona"), ..], where for every row with attribute 1 the second row contains Arizona
    # If mapping is correct, append a 1, if incorrect append a 0, and count how often 0 occurs
    incorrect_mapping_count = rdd_combined.map(lambda row: (row, 1) if row[1] == attribute_mapping.get(row[0]) else (row, 0)) \
                                .filter(lambda row: row[1] == 0) \
                                .count() 

    print(incorrect_mapping_count)


if __name__ == '__main__':
    main()

