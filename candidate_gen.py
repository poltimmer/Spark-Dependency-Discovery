from DependencyFinder import setup_header, soft_fd, delta_fd
import itertools
import time
from warnings import warn

FILE_PATH = "/group/mcs-2amd15-15/VR_20051125_Post"  # <-- file path when using hpc


def main(nr_of_columns, nr_of_rows=None, tau=None, delta=None):
    header, input_rdd = setup_header(FILE_PATH, nr_of_rows)
    columns = list(header.split('\t'))[:nr_of_columns]

    print(">> START MAIN: Running for {} rows, {} columns, tau {}, delta {}"
          .format(nr_of_rows if nr_of_rows is not None else "all", nr_of_columns, tau, delta))

    min_fd, min_soft_fd, min_delta_fd = {}, {}, {}
    for col in columns:
        min_fd[col] = []
        min_soft_fd[col] = []
        min_delta_fd[col] = []

    if tau is not None:
        initial_time = time.time()
        for depth in range(1, 4):
            depth_time = time.time()
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

            next_gen_candidates = list(candidates)

            # iterative sampling
            for factor in [2, 5, 10, 25]:
                frac_time = time.time()
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
                old_candidates = next_gen_candidates
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

                print(">> Columns: {}, Runtime: {}, frac: {}, amount_of_candidates (current): {}"
                      .format(nr_of_columns, time.time() - frac_time, frac, len(old_candidates)))
                if len(next_gen_candidates) == 0:
                    break

            p_values_map = soft_fd(next_gen_candidates, columns, input_rdd)  # [((('A'), 'B'), 0.97)]

            # Build min_fd and min_soft_fd lists
            for (lhs, rhs), p_val in p_values_map:
                if p_val == 1:  # case 1: hard functional dependency
                    min_fd[rhs].append((lhs, rhs))
                elif p_val >= tau:  # case 2: soft functional dependency
                    if all(map(lambda x: not set(x).issubset(set(lhs)),
                           map(lambda fd: fd[0], min_fd[key]))):
                        min_soft_fd[rhs].append((lhs, rhs))

            print(">> Runtime fds: {} for columns {} for depth: {}, ".format(time.time() - depth_time, nr_of_columns, depth))
        print(">> TOTAL Soft FD runtime: {} for columns {}||".format(time.time() - initial_time, nr_of_columns))
        print("Minimal functional dependencies: ", min_fd)
        print("Minimal soft dependencies: ", min_soft_fd, "\n")

    if delta is not None:
        initial_time = time.time()
        for depth in range(1, 4):
            depth_time = time.time()
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
                frac_time = time.time()
                distance_values_map = delta_fd(
                    next_gen_candidates,
                    delta,
                    columns,
                    input_rdd,
                    frac)  # [((('A'), 'B'), 2)]
                old_candidates = next_gen_candidates
                next_gen_candidates = []
                # print(f"Frac: {frac}, next_gen_candidates: {next_gen_candidates}")
                for (lhs, rhs), dist in distance_values_map:
                    if dist <= delta:
                        next_gen_candidates.append((lhs, rhs))

                print(f"Frac: {frac}, runtime:{time.time() - frac_time},  # amount of current candidates: {len(old_candidates)}, columns: {nr_of_columns}, delta: {delta}")
                if len(next_gen_candidates) == 0:
                    break

            delta_fd(next_gen_candidates, delta, columns, input_rdd)
            # Build min_fd and min_soft_fd lists
            for (lhs, rhs), dist in distance_values_map:
                if dist <= delta:
                    min_delta_fd[rhs].append((lhs, rhs))
            print(">> Depth {}: Delta FD runtime: {}, columns: {} delta: {}".format(depth, time.time() - depth_time, nr_of_columns, delta))
        print(">> TOTAL Delta FD runtime: {} delta: {}".format(time.time() - initial_time, delta))
        print("Minimal delta dependencies: ", min_delta_fd)
        print("==========================END LOOP==============================\n")


if __name__ == '__main__':
    default_tau = 0.995
    default_delta = 1
    for i in [5, 7, 10, 12, 15]:
        main(nr_of_columns=i, tau=default_tau, delta=default_delta)
    for i in range(1, 5):
        main(nr_of_columns=10, tau=None, delta=i)
