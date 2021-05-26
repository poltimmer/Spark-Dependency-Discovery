from pyspark.sql import SparkSession

path = "C:\\Users\\Public\\VR_20051125_Post"
columns = ["county_id", "status_cd", "reason_cd", "last_name", "first_name", "midl_name", "house_num", "half_code",
           "street_dir", "street_name", "street_type_cd", "unit_num", "res_city_desc", "state_cd", "zip_code",
           "mail_city", "mail_state", "mail_zipcode", "area_cd", "phone_num", "race_code", "ethnic_code", "party_cd",
           "sex_code", "age", "birth_place", "precinct_desc", "municipality_desc", "ward_desc", "cong_dist_desc",
           "super_court_desc", "judic_dist_desc", "NC_senate_desc", "NC_house_desc", "county_commiss_desc",
           "township_desc", "school_dist_desc", "fire_dist_desc", "water_dist_desc", "sewer_dist_desc",
           "sanit_dist_desc", "rescue_dist_desc", "munic_dist_desc", "dist_1_desc", "age_group"]

sample_input = [
    (("age_group"), "age"),
    (("age"), "age_group")
]


def main():
    spark: SparkSession = SparkSession.builder \
        .master("local[*]") \
        .appName("SparkDB") \
        .getOrCreate()

    rdd = spark.sparkContext.textFile(path)
    #header = rdd.first()
    #rdd = rdd.filter(lambda row: row != header)

    soft_fd_result = soft_fd(rdd, sample_input)

    print(soft_fd_result)
    return soft_fd_result


def map_tuples_to_combined_rdd(row, tuples):
    new_rows = []
    for i, tup in enumerate(tuples):
        A = tup[0]
        B = tup[1]
        key = str(i) + ";"

        # Get indices for desired columns
        if type(A) is str:
            idx = [columns.index(A)]
        else:
            idx = [columns.index(col) for col in A]

        row_split = row.split("\t")
        for id in idx:
            key += row_split[id]
        key = key + ";" + row_split[columns.index(B)]

        # new row of format ("tuple;A;B", 1),
        # for example ("((age_group), age);41 TO 64;62", 1) or ("((age_group, county_id), age);41 TO 6418;62", 1)
        new_rows.append((key, 1))

    return new_rows


def soft_fd(rdd, tup):
    """
    # geen fd
    ("a_1, b_1") ("a_2, b_2") ("a_2, b_1")

    ("a_1;b_1", 1) ("a_2; b_2", 1) ("a_1;b_2", 1)
    ("a_1", 2), ("a_2", 1)  # (a_i, N)
    ("a_i" m/N) : ("a_1", 1/2), ("a_2", 1/1) --> 0.95

    m = 1 # maximum van a,b combinatie counts

    # wel fd
    ("a_1, b_1") ("a_2, b_2") ("a_2, b_2")

    ("a_1;b_1", 1) ("a_2; b_2", 2)
    ("a_1", 1), ("a_2", 2)  # (a_i, N)
    ("a_i" m/N) : ("a_1", 2/1), ("a_2", 2/2) --> 1/1

    m = 2 # maximum van a,b combinatie counts
    """

    # compute the number of occurrences of unique a, b combinations per input tuple
    a_b_combi_counts = rdd.flatMap(lambda row: map_tuples_to_combined_rdd(row, sample_input)) \
        .reduceByKey(lambda a, b: a + b)

    # remove ;A;B from the key, so only the input tuple remains and retrieving the maximum count per tuple
    maximum_of_a_b_combination_counts = dict(
        a_b_combi_counts.map(lambda pair: (int(pair[0].split(";")[0]), pair[1])) \
            .reduceByKey(lambda count_1, count_2: count_1 if count_1 > count_2 else count_2) \
            .collect()
    )

    # Count the number of unique values in A per input tuple
    unique_a_counts = a_b_combi_counts.map(
        lambda a_b_combi_count: (a_b_combi_count[0].split(";")[0] + ";" + a_b_combi_count[0].split(";")[1], a_b_combi_count[1])  # remove the B
    ).reduceByKey(lambda count_a_i_row1, count_a_i_row2: count_a_i_row1 + count_a_i_row2)

    def compute_P(pair):
        idx = int(pair[0].split(";")[0])
        value = pair[1]
        m = maximum_of_a_b_combination_counts.get(idx)
        P = m / value
        return (idx, P)

    # compute the fraction (P) per A per input tuple
    fraction_per_a = unique_a_counts.map(lambda a_i_count: compute_P(a_i_count))

    # Reduce tuple id to obtain the minimum P per tuple.
    P_min = fraction_per_a.reduceByKey(
        lambda a_i_fraction, a_j_fraction: a_i_fraction if a_i_fraction < a_j_fraction else a_j_fraction
    ).collect()

    return P_min


def delta_fd(rdd, tup):
    pass
    # inner_join = rdd.join(rdd)  #.map(lambda row: (row[0], row[1]))
    # max_distance = inner_join.map(lambda row[])  #edit_distance.SequenceMatcher(a="test", b="tesd").distance()
    # return None


if __name__ == '__main__':
    main()
