from pyspark import StorageLevel
from pyspark.sql import SparkSession
import pickle

input_file_path = "C:\\Users\\20173995\\Desktop\\VR_20051125.txt"
output_folder_path = "C:\\Users\\Public\\Data"
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
col_names_bitmap = [1] * len(col_names)  # bit map corresponding to col_names, initially all ones

# Enter attribute names which should always be removed
unwanted_columns = ["snapshot_dt", "county_desc", "voter_reg_num", "ncid", "voter_status_desc",
                    "voter_status_reason_desc", "absent_ind", "name_prefx_cd", "name_sufx_cd", "street_sufx_cd",
                    "unit_designator", "mail_addr1", "mail_addr2", "mail_addr3", "mail_addr4", "race_desc",
                    "ethnic_code", "party_desc", "sex_code", "registr_dt", "precinct_abbrv", "municipality_abbrv",
                    "ward_abbrv", "cong_dist_abbrv", "super_court_abbrv", "judic_dist_abbrv", "NC_senate_abbrv",
                    "NC_house_abbrv", "county_commiss_abbrv", "township_abbrv", "school_dist_abbrv", "fire_dist_abbrv",
                    "water_dist_abbrv", "sewer_dist_abbrv", "sanit_dist_abbrv", "rescue_dist_abbrv", "munic_dist_abbrv",
                    "dist_1_abbrv", "dist_2_abbrv", "dist_2_desc", "confidential_ind", "cancellation_dt",
                    "vtd_abbrv", "vtd_desc", "load_dt"]


def main():
    if input_file_path == "":
        print("Please provide an input file")
        return 0
    elif output_folder_path == "":
        print("No output folder specified")
        return 0

    # spark init code
    spark: SparkSession = SparkSession.builder \
        .master("local[4]") \
        .appName("SparkDB") \
        .getOrCreate()

    rdd = spark.sparkContext.textFile(input_file_path)
    print("Original rdd size: %d" % rdd.count())

    # Remove badly formated rows and whitespace
    clean_rdd = rdd.filter(lambda row: len(row.split("\t")) == len(col_names)) \
        .map(lambda row: strip_white_space(row))

    print("Removed the following rows due to bad formatting:\n",
          rdd.filter(lambda row: len(row.split("\t")) != len(col_names)).collect())

    # preprocess on the rdd without header
    header = clean_rdd.first()
    desired_columns = col_names_bitmap # preprocess(clean_rdd.filter(lambda row: row != header))

    # Check and correct whether all attributes in the unwanted_columns array will be removed
    for attr in unwanted_columns:
        if col_names_bitmap[col_names.index(attr)] != 0:
            col_names_bitmap[col_names.index(attr)] = 0

    def slice_rdd(row):
        arr = row.split("\t")
        first = desired_columns.index(1)
        result = arr[first]
        for i in range(first + 1, len(desired_columns)):
            if desired_columns[i]:
                result += "\t" + arr[i]
        return result

    output_rdd = clean_rdd.map(lambda row: slice_rdd(row))

    output_rdd.coalesce(1).saveAsTextFile(output_folder_path)

    spark.stop()


def strip_white_space(row):
    arr = row.split("\t")
    result = arr[0]
    for i in range(1, len(arr)):
        result += "\t" + arr[i].strip()
    return result


def unique_check(file):
    return file.distinct().count()


def null_check(file):
    null_count = file.filter(lambda row: row == '').count()
    return null_count


def one_to_one_mapping_check(file, file_neighbour, file_combined):
    left_word_count = file \
        .map(lambda row: (row, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .collect()

    right_word_count = file_neighbour \
        .map(lambda row: (row, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .collect()

    # If the lengths of both array are not the same, they cannot map one-to-one
    if len(left_word_count) != len(right_word_count):
        return False

    # Sort on number of occurrences
    left_sorted = sorted(left_word_count, key=lambda x: x[1])
    right_sorted = sorted(right_word_count, key=lambda x: x[1])
    attribute_mapping = {left[0]: right[0] for left, right in zip(left_sorted, right_sorted)}

    if all(attribute_mapping):
        incorrect_mapping_count = file_combined.map(
            lambda row: (row, 1) if row[1] == attribute_mapping.get(row[0]) else (row, 0)) \
            .filter(lambda row: row[1] == 0) \
            .count()
        return incorrect_mapping_count == 0
    else:
        return False


def preprocess(file):
    file.persist(StorageLevel.DISK_ONLY)  # StorageLevel can be changed dependent on the setup
    file_row_count = file.count()
    results = {'total_rows': file_row_count}
    print("Input file row count: %d" % file_row_count)

    skip_one = False  # initially don't skip any iteration

    for i in reversed(range(0, len(col_names))):
        name = col_names[i]  # enumerate is better, but janky python doesn't work well with skipping over enumerations.
        # if skip_one is true, skip the current iteration
        if skip_one:
            skip_one = False  # Reset boolean
            results.update({name: {"skipped": True}})
            col_names_bitmap[i] = 0
            print("skip one:", name)
            continue

        print('checking for column: {}'.format(name))

        # obtain rdd of current attribute and neighbouring attribute
        attr_rdd = file.map(lambda row: row.split("\t")[i])

        # Check for the number of unique values, if there is only one, then we store it and continue the next ieration
        unique = unique_check(attr_rdd)
        results.update({name: {'unique_vals': unique}})
        print("Attribute %s contains %d unique value(s)" % (name, unique))
        if unique == 1 or unique == file_row_count:
            col_names_bitmap[i] = 0
            continue

        null_values = null_check(attr_rdd)
        results.update({name: {'null_values': null_values}})
        print("Attribute %s contains %d unique value(s)" % (name, unique))

        if i != len(col_names) - 1:
            attr_neighbour_rdd = file.map(lambda x: x.split("\t")[i + 1])
            combined_rdd = file.map(lambda x: x.split("\t")[i:i + 2])

            neighbour_mapping = one_to_one_mapping_check(attr_rdd, attr_neighbour_rdd, combined_rdd)
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
