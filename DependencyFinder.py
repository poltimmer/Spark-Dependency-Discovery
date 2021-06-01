from pyspark.sql import SparkSession

# Seed used for sampling
SEED = 420


def setup_header(file_path, nr_of_rows=None):
    spark: SparkSession = SparkSession.builder \
        .master("local[*]") \
        .appName("SparkDB") \
        .getOrCreate()

    input_rdd = spark.sparkContext.textFile(file_path)
    header = input_rdd.first()
    input_rdd = input_rdd.filter(lambda row: row != header)

    if nr_of_rows is not None:
        input_rdd = input_rdd.map(lambda x: (x, )).toDF().limit(int(nr_of_rows)).rdd.map(list)

    return header, input_rdd


def map_tuples_to_combined_rdd(row, tuples, header):
    new_rows = []
    for i, tup in enumerate(tuples):
        A = tup[0]
        B = tup[1]
        key = str(i) + ";"

        # Get indices for desired columns
        if type(A) is str:
            idx = [header.index(A)]
        else:
            idx = [header.index(col) for col in A]

        row_split = row.split("\t")
        for id in idx:
            key += row_split[id]
        key = key + ";" + row_split[header.index(B)]

        # new row of format ("tuple;A;B", 1),
        # for example ("((age_group), age);41 TO 64;62", 1) or ("((age_group, county_id), age);41 TO 6418;62", 1)
        new_rows.append((key, 1))

    return new_rows


def soft_fd(candidates, header, input_rdd, sample=None):
    if sample is not None:
        input_rdd = input_rdd.sample(withReplacement=False, fraction=sample, seed=SEED)

    # compute the number of occurrences of unique a, b combinations per input tuple
    a_b_combi_counts = input_rdd.flatMap(lambda row: map_tuples_to_combined_rdd(row, candidates, header)) \
        .reduceByKey(lambda a, b: a + b)

    # remove ;A;B from the key, so only the input tuple remains and retrieving the maximum count per tuple
    N_count_per_A = a_b_combi_counts \
            .map(lambda pair: (pair[0].split(";")[0] + ";" + pair[0].split(";")[1], pair[1])) \
            .reduceByKey(lambda N_1, N_2: N_1 + N_2) \
            .map(lambda pair: (pair[0].split(";")[0], pair[1]))  # Remove A from the key, such that join can be performed

    max_m_per_candidate = a_b_combi_counts \
            .map(lambda pair: (pair[0].split(";")[0], pair[1])) \
            .reduceByKey(lambda m_1, m_2: m_1 if m_1 > m_2 else m_2)

    joined_m_N = max_m_per_candidate.join(N_count_per_A)

    # compute the fraction (P) per A per input tuple
    fraction_per_a = joined_m_N.map(lambda row: (int(row[0]), row[1][0] / row[1][1]))

    # Reduce tuple id to obtain the minimum P per tuple.
    P_min = fraction_per_a.reduceByKey(
        lambda a_i_fraction, a_j_fraction: a_i_fraction if a_i_fraction < a_j_fraction else a_j_fraction
    )

    return [(candidates[idx], p) for idx, p in P_min.collect()]


def delta_fd(candidates, delta, header, input_rdd, sample):
    if sample is not None:
        input_rdd = input_rdd.sample(withReplacement=False, fraction=sample, seed=SEED)

    # same step as the first step from fd's:     
    # compute the number of occurrences of unique a, b combinations per input tuple
    a_b_combi_counts = input_rdd.flatMap(lambda row: map_tuples_to_combined_rdd(row, candidates, header)) \
        .reduceByKey(lambda a, b: a + b)

    # Here, we go from   ("((age_group), age);41 TO 64;62", //count (e.g. 1)//),  to "("((age_group), age);41 TO 64","62"), enabling 'step 2A' in report
    a_r_pairs = a_b_combi_counts \
        .map(lambda pair: (pair[0].split(";")[0] + ";" + pair[0].split(";")[1], pair[0].split(";")[2]))  # remove the count leaving only unique values
    #output : "("((age_group), age);41 TO 64", "69")

    inner_join = a_r_pairs.join(a_r_pairs)  # computing (S_A, (a_k, a_k)), example ('1;7', ('17 AND BELOW', '17 AND BELOW'))

    #https://stackoverflow.com/questions/59686989/levenshtein-distance-with-bound-limit
    def levenshtein(s1, s2, maximum): 
        if len(s1) > len(s2):
            s1, s2 = s2, s1

        distances = range(len(s1) + 1)
        for i2, c2 in enumerate(s2):
            distances_ = [i2+1]
            for i1, c1 in enumerate(s1):
                if c1 == c2:
                    distances_.append(distances[i1])
                else:
                    distances_.append(1 + min((distances[i1], distances[i1 + 1], distances_[-1])))
            if all((x >= maximum for x in distances_)):
                return maximum #False #TODO Define properly
            distances = distances_
        return distances[-1]

    def findDist(row, delta):
        key = row[0].split(';')[0]  # reduce key to only contain candidate id ((age_group), age);41 TO 64; -> ((age_group), age)
        compare_left, compare_right = row[1]
        dist = levenshtein(compare_left, compare_right, delta + 1)
        return (key, dist)
        
    distance_mapping = inner_join.map(lambda row: findDist(row, delta)) \
        .reduceByKey(lambda row1_dist, row2_dist: row1_dist if row1_dist > row2_dist else row2_dist) \
        .collect()

    return [(candidates[int(idx)], int(dist)) for idx, dist in distance_mapping]

# columns = ["county_id", "status_cd", "reason_cd", "last_name", "first_name", "middle_name", "house_num", "half_code",
#            "street_dir", "street_name", "street_type_cd", "unit_num", "res_city_desc", "state_cd", "zip_code",
#            "mail_city", "mail_state", "mail_zipcode", "area_cd", "phone_num", "race_code", "ethnic_code", "party_cd",
#            "sex_code", "age", "birth_place", "precinct_desc", "municipality_desc", "ward_desc", "cong_dist_desc",
#            "super_court_desc", "judic_dist_desc", "NC_senate_desc", "NC_house_desc", "county_commiss_desc",
#            "township_desc", "school_dist_desc", "fire_dist_desc", "water_dist_desc", "sewer_dist_desc",
#            "sanit_dist_desc", "rescue_dist_desc", "munic_dist_desc", "dist_1_desc", "age_group"]

# sample_input = [
#     (("zip_code", "house_num"), "street_name"), 
#     (("reason_cd"), "status_cd"), (("race_code"), "ethnic_code"), 
#     (("zip_code", "house_num", "unit_num"), "last_name"), 
#     (("first_name"), "sex_code"), (("street_name"), "street_type_cd"),
#     (("status_cd"), "reason_cd"),
#     (("zip_code", "house_num", "unit_num"), "last_name"),
#     (("first_name"), "sex_code"),
#     (("street_name"), "street_type_cd"),
#     (("age_group"), "age"), (("age"), "age_group")
# ]
