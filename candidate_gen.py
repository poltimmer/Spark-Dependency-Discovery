from DependencyFinder import setup_header, soft_fd, delta_fd
import itertools
import time
from warnings import warn
FILE_PATH = "/group/mcs-2amd15-15/VR_20051125_Post"  # <-- file path when using hpc


def main(nr_of_columns, nr_of_rows=None, tau=None, delta=None):
    header, input_rdd = setup_header(FILE_PATH, nr_of_rows)
    columns = list(header.split('\t'))[:nr_of_columns]

    print("Running for {} rows, {} columns, tau {}, delta {}"
          .format(nr_of_rows if nr_of_rows is not None else "all", len(columns), tau, delta))

    min_fd, min_soft_fd, min_delta_fd = {}, {}, {}
    for col in columns:
        min_fd[col] = []
        min_soft_fd[col] = []
        min_delta_fd[col] = []

    if tau is not None:
        initial_time = time.time()
        for depth in range(1, 4):
            perms = {}
            # Create permutations for current layer
            for col in columns:
                perms[col] = [(left, col) for left in itertools.combinations(
                    [c for c in columns if c != col], depth)]
            candidates = []
            for key, partition in perms.items():
                for candidate in partition:
                    if all(map(lambda x: not set(x).issubset(
                            set(candidate[0])), map(lambda fd: fd[0], min_fd[key]))):
                        candidates.append(candidate)

            # print('>> checking p values for {} candidates ||'.format(len(candidates)))
            # start_time = time.time()

            next_gen_candidates = list(candidates)

            # iterative sampling
            for factor in [2, 5, 10, 25]:
                frac = factor * 2 * (1 - tau)
                if frac >= 1:
                    warn(
                        f"[warning] - Sampling fraction {frac} below minimum threshold given tau ({tau}), skipping fraction")
                    continue
                p_values_map = soft_fd(
                    next_gen_candidates,
                    columns,
                    input_rdd,
                    frac)  # [((('A'), 'B'), 2)]
                next_gen_candidates = []
                for (lhs, rhs), p_val in p_values_map:
                    # p-val is 0.9
                    # tau is 0.95
                    # frac is 1*2*(1-0.95)=0.1
                    # tau / frac = 9.5
                    # 0.9 > 9.5 = false

                    # 1

                    # tau = 0.99 (klopt de dependency in 99% van de data?)
                    # 1-tau = 0.01 (in maximimaal 1% van de data mag het niet kloppen)
                    # frac =  0.1 (we samplen 10% van de data)
                    # p_val >= 1 - ((1-tau) * (fraction)) (als dit waar is, dan kunnen we hem niet uitsluiten)
                    # 10% 1% ->
                    if p_val >= 1 - ((1 - tau) * (frac)):
                        next_gen_candidates.append((lhs, rhs))

            p_values_map = soft_fd(
                next_gen_candidates,
                columns,
                input_rdd)  # [((('A'), 'B'), 0.97)]
            # print(">> Runtime: {}".format(time.time() - start_time))
            # print('>> p values map for iter {}: {} ||'.format(depth, p_values_map))

            # Build min_fd and min_soft_fd lists
            for (lhs, rhs), p_val in p_values_map:
                if p_val == 1:  # case 1: hard functional dependency
                    min_fd[rhs].append((lhs, rhs))
                elif p_val >= tau:  # case 2: soft functional dependency
                    if all(map(lambda x: not set(x).issubset(set(lhs)),
                           map(lambda fd: fd[0], min_fd[key]))):
                        min_soft_fd[rhs].append((lhs, rhs))

        print(">> Soft FD runtime: {} ||".format(time.time() - initial_time))
        print("Minimal functional dependencies: ", min_fd)
        print("Minimal soft dependencies: ", min_soft_fd)

    if delta is not None:
        # TODO: Duplicate code between delta and soft candidate generation could be reduced
        initial_time = time.time()
        for depth in range(1, 4):
            perms = {}
            # Create permutations for current layer
            for col in columns:
                perms[col] = [(left, col) for left in itertools.combinations(
                    [c for c in columns if c != col], depth)]
            candidates = []
            for key, partition in perms.items():
                for candidate in partition:
                    if all(map(lambda x: not set(x).issubset(set(candidate[0])), map(lambda fd: fd[0], min_fd[key]))) \
                            and all(map(lambda x: not set(x).issubset(set(candidate[0])), map(lambda d_fd: d_fd[0], min_delta_fd[key]))):
                        candidates.append(candidate)

            next_gen_candidates = list(candidates)
            for frac in [0.0001, 0.001, 0.01, 0.05, 0.10]:
                distance_values_map = delta_fd(
                    next_gen_candidates,
                    delta,
                    columns,
                    input_rdd,
                    frac)  # [((('A'), 'B'), 2)]
                next_gen_candidates = []
                # print(f"Frac: {frac}, next_gen_candidates: {next_gen_candidates}")
                for (lhs, rhs), dist in distance_values_map:
                    if dist <= delta:
                        next_gen_candidates.append((lhs, rhs))
                print(f"Frac: {frac}, # next_gen_candidates: {len(next_gen_candidates)}")
            delta_fd(next_gen_candidates, delta, columns, input_rdd)
            # Build min_fd and min_soft_fd lists
            for (lhs, rhs), dist in distance_values_map:
                if dist <= delta:
                    min_delta_fd[rhs].append((lhs, rhs))

        print(">> Delta FD runtime: {} ||".format(time.time() - initial_time))
        print("Minimal delta dependencies: ", min_delta_fd)


if __name__ == '__main__':
    default_tau = 0.999
    default_delta = 1
    main(nr_of_columns=10, tau=default_tau, delta=default_delta)

    '''
    # Performance measurement runs
    nr_of_rows_set = [5e5, 1e6, 2e6, 4e6, 8e6]
    for rows in nr_of_rows_set:
        main(nr_of_columns=6, nr_of_rows=rows, tau=default_tau, delta=default_delta)

    nr_of_columns_set = [4, 6, 8, 10, 12, 14]
    for columns in nr_of_columns_set:
        main(nr_of_columns=columns, nr_of_rows=1e6, tau=default_tau, delta=default_delta)

    tau_set = [0.5, 0.8, 0.9, 0.95, 0.99]
    for tau in tau_set:
        main(nr_of_columns=6, nr_of_rows=8e6, tau=tau, delta=None)

    delta_set = [1, 2, 3, 5, 10]
    for delta in delta_set:
        main(nr_of_columns=6, nr_of_rows=8e6, tau=None, delta=delta)
    '''
