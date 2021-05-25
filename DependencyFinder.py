from pyspark.sql import SparkSession

path = "/home/tommie/Downloads/VR_20051125_Post"
columns = ["county_id", "status_cd", "reason_cd", "last_name", "first_name", "middle_name", "house_num", "half_code",
           "street_dir", "street_name", "street_type_cd", "unit_num", "res_city_desc", "state_cd", "zip_code",
           "mail_city", "mail_state", "mail_zipcode", "area_cd", "phone_num", "race_code", "ethnic_code", "party_cd",
           "sex_code", "age", "birth_place", "precinct_desc", "municipality_desc", "ward_desc", "cong_dist_desc",
           "super_court_desc", "judic_dist_desc", "NC_senate_desc", "NC_house_desc", "county_commiss_desc",
           "township_desc", "school_dist_desc", "fire_dist_desc", "water_dist_desc", "sewer_dist_desc",
           "sanit_dist_desc", "rescue_dist_desc", "munic_dist_desc", "dist_1_desc", "age_group"]

sample_input = [
    (("zip_code", "house_num"), "street_name"), 
    (("reason_cd"), "status_cd"), (("race_code"), "ethnic_code"), 
    (("zip_code", "house_num", "unit_num"), "last_name"), 
    (("first_name"), "sex_code"), (("street_name"), "street_type_cd"),
    (("status_cd"), "reason_cd"),
    (("zip_code", "house_num", "unit_num"), "last_name"),
    (("first_name"), "sex_code"),
    (("street_name"), "street_type_cd"),
    (("age_group"), "age"), (("age"), "age_group")
]

def main():
    spark: SparkSession = SparkSession.builder \
        .master("local[*]") \
        .appName("SparkDB") \
        .getOrCreate()

    rdd = spark.sparkContext.textFile(path)
    header = rdd.first()
    rdd = rdd.filter(lambda row: row != header)

    result = []

    for tup in sample_input:
        P, word_count = soft_fd(rdd, tup)

        if P == 1:
            result.append((tup, P, 0))
        else:
            D = delta_fd(word_count, tup)
            result.append((tup, P, D))

    print(result)
    return result


def map_tuple(row, attr):
    A = attr[0]
    B = attr[1]
    key = ""

    # Get indices for desired columns
    if type(A) is str:
        idx = [columns.index(A)]
    else:
        idx = [columns.index(col) for col in A]

    row_split = row.split("\t")
    for id in idx:
        key += row_split[id]
    key = key + ";" + row_split[columns.index(B)]

    return (key, 1)


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

    a_b_combi_counts = rdd.map(lambda row: map_tuple(row, tup)) \
                    .reduceByKey(lambda a, b: a + b).cache()

    maximum_of_a_b_combination_counts: int = a_b_combi_counts.reduce(lambda row1, row2: row1 if row1[1] > row2[1] else row2)[1]

    unique_a_counts = a_b_combi_counts.map(
        lambda a_b_combi_count: (a_b_combi_count[0].split(";")[0], a_b_combi_count[1])
    ).reduceByKey(
        lambda count_a_i_row1, count_a_i_row2: count_a_i_row1 + count_a_i_row2
    )

    fraction_per_a = unique_a_counts.map(lambda a_i_count: (a_i_count[0], maximum_of_a_b_combination_counts / a_i_count[1]))

    P_min: float = fraction_per_a.reduce(lambda a_i_fraction, a_j_fraction: a_i_fraction if a_i_fraction[1] < a_j_fraction[1] else a_j_fraction)[1]

    return P_min, a_b_combi_counts


def delta_fd(rdd, tup):
    pass
    # inner_join = rdd.join(rdd)  #.map(lambda row: (row[0], row[1]))
    # max_distance = inner_join.map(lambda row[])  #edit_distance.SequenceMatcher(a="test", b="tesd").distance()
    # return None


if __name__ == '__main__':
    main()