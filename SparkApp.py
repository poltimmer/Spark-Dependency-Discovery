from pyspark import StorageLevel
from pyspark.sql import SparkSession
import pickle

col_names = ["snapshot_dt", "county_id", "county_desc", "voter_reg_num", "ncid", "status_cd", "voter_status_desc",
             "reason_cd", "voter_status_reason_desc", "absent_ind", "name_prefx_cd", "last_name", "first_name",
             "midl_name", "name_sufx_cd", "house_num", "half_code", "street_dir", "street_name", "street_type_cd",
             "street_sufx_cd", "unit_designator", "unit_num", "res_city_desc", "state_cd", "zip_code", "mail_addr1",
             "mail_addr2", "mail_addr3", "mail_addr4", "mail_city", "mail_state", "mail_zipcode", "area_cd",
             "phone_num", "race_code", "race_desc", "ethnic_code", "ethnic_desc", "party_cd", "party_desc", "sex_code",
             "sex", "age", "birth_place", "registr_dt", "precinct_abbrv", "precinct_desc", "municipality_abbrv",
             "municipality_desc", "ward_abbrv", "ward_desc", "cong_dist_abbrv", "cong_dist_desc", "super_court_abbrv",
             "super_court_desc", "judic_dist_abbrv", "judic_dist_desc", "NC_senate_abbrv", "NC_senate_desc",
             "NC_house_abbrv", "NC_house_desc", "county_commiss_abbrv", "county_commiss_desc", "township_abbrv",
             "township_desc", "school_dist_abbrv", "school_dist_desc", "fire_dist_abbrv", "fire_dist_desc",
             "water_dist_abbrv", "water_dist_desc", "sewer_dist_abbrv", "sewer_dist_desc", "sanit_dist_abbrv",
             "sanit_dist_desc", "rescue_dist_abbrv", "rescue_dist_desc", "munic_dist_abbrv", "munic_dist_desc",
             "dist_1_abbrv", "dist_1_desc", "dist_2_abbrv", "dist_2_desc", "confidential_ind", "cancellation_dt",
             "vtd_abbrv", "vtd_desc", "load_dt", "age_group"]


def main():
    # spark init code
    spark: SparkSession = SparkSession.builder \
        .master("local[4]") \
        .appName("SparkDB") \
        .getOrCreate()

    # rdd = spark.sparkContext.textFile("./data/VR_20051125.txt")
    rdd = spark.sparkContext.textFile("C:/Users/20173995/Desktop/VR_20060210.txt")
    print("Original rdd size: %d" % rdd.count())

    # Remove the header and badly formated rows
    header = rdd.first()
    filtered_rdd = rdd.filter(lambda row: row != header) \
                      .filter(lambda row: len(row.split("\t")) == len(col_names))

    print("Removed the following rows due to bad formatting:\n", rdd.filter(lambda row: len(row.split("\t")) != len(col_names)).take(4))

    desired_columns = preprocess(filtered_rdd)

    def slice_rdd(row):
        arr = row.split("\t")
        first = desired_columns.index(1)
        result = arr[first].strip()
        for i in range(first + 1, len(desired_columns)):
            if desired_columns[i]:
                result += "\t" + arr[i].strip()
        return result

    output_rdd = rdd.map(lambda row: slice_rdd(row))

    # output_rdd.coalesce(1).saveAsTextFile("./out/out")
    output_rdd.coalesce(1).saveAsTextFile("C:/Users/Public/data")

    spark.stop()


def unique_check(file):
    return file.distinct().count()


def null_check(file):
    null_count = file.filter(lambda row: row == '').count()
    return null_count


def one_to_one_mapping_check(file, file_neighbour, file_combined):
    left_word_count = file \
        .map(lambda row: (row.strip(), 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .collect()

    right_word_count = file_neighbour \
        .map(lambda row: (row.strip(), 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .collect()

    print(len(left_word_count), len(right_word_count))
    print(left_word_count)
    print(right_word_count)

    if len(left_word_count) != len(right_word_count):
        return False

    # Sort on number of occurrences
    left_sorted = sorted(left_word_count, key=lambda x: x[1])
    right_sorted = sorted(right_word_count, key=lambda x: x[1])
    attribute_mapping = {left[0]: right[0] for left, right in zip(left_sorted, right_sorted)}

    if all(attribute_mapping):
        # Mapping array is of format [("1", "Arizona"), ..], where for every row with attribute 1 the second row contains Arizona
        # If mapping is correct, append a 1, if incorrect append a 0, and count how often 0 occurs
        incorrect_mapping_count = file_combined.map(
            lambda row: (row, 1) if row[1] == attribute_mapping.get(row[0].strip()) else (row, 0)) \
            .filter(lambda row: row[1] == 0) \
            .count()
        return incorrect_mapping_count == 0
    else:
        return False


def preprocess(file):
    col_names_bitmap = [1] * len(col_names)  # initial bit map
    file.persist(StorageLevel.DISK_ONLY)  # StorageLevel can be changed dependent on the setup
    file_row_count = file.count()
    results = {'total_rows': file_row_count}
    print("Input file row count: %d" % file_row_count)

    skip_one = False  # initially don't skip any iteration

    for i in range(0, len(col_names)):
        name = col_names[i]  # enumerate is better, but janky python doesn't work well with skipping over enumerations.
        # if skip_one is true, skip the current iteration
        if skip_one:
            skip_one = False  # Reset boolean
            # results.update({name: {"skipped": True}})
            col_names_bitmap[i] = 0
            print("skip one:", name)
            continue

        print('checking for column: {}'.format(name))

        # obtain rdd of current attribute and neighbouring attribute
        attr_rdd = file.map(lambda row: row.split("\t")[i])

        # Check for the number of unique values, if there is only one, then we store it and continue the next ieration
        unique = unique_check(attr_rdd)
        results.update({name: {'unique_vals': unique}})
        if unique == 1 or unique == file_row_count:
            col_names_bitmap[i] = 0
            print("Attribute %s contains %d unique value(s)" % (name, unique))
            continue

        if i != len(col_names) - 1:
            attr_neighbour_rdd = file.map(lambda x: x.split("\t")[i+1])
            combined_rdd = file.map(lambda x: x.split("\t")[i:i+2])

            neighbour_mapping = one_to_one_mapping_check(attr_rdd, attr_neighbour_rdd, combined_rdd)
            print("Result:", neighbour_mapping)
            results[name].update({"neighbours_check": neighbour_mapping})
            if neighbour_mapping:
                print("Attribute %s and %s map one-to-one" % (name, col_names[i + 1]))
                skip_one = True
    results.update({'bitmap': col_names_bitmap})
    print(results)

    with open('results.pickle', 'wb') as handle:
        pickle.dump(results, handle, protocol=pickle.HIGHEST_PROTOCOL)

    return col_names_bitmap


if __name__ == '__main__':
    main()
