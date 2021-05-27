import random
from DependencyFinder import setup_header, soft_fd
import itertools
import pickle
import time

TAU = 0.98
FILE_PATH = "/home/mcs001/shared_data/mcs-2amd15/mcs-2amd15-15/VR_20051125_Post"
DELTA = 2  # can be done


def main():
    header, input_rdd = setup_header(FILE_PATH)
    columns = list(header.split('\t'))[:2]  # TODO remove [:5]

    print(columns)
    min_fd = {}
    for col in columns:
        min_fd[col] = []

    min_soft_fd = {}
    for col in columns:
        min_soft_fd[col] = []

    initial_time = time.time()
    for depth in range(1, 4):
        # TODO: add a check and to break if there are no candidates
        perms = {}
        # Create permutations for current layer
        for col in columns:
            perms[col] = [(left, col) for left in itertools.combinations([c for c in columns if c != col], depth)]
        candidates = []
        for key, partition in perms.items():
            for candidate in partition:
                if all(map(lambda x: not set(x).issubset(set(candidate[0])), map(lambda fd: fd[0], min_fd[key]))):
                    candidates.append(candidate)

        print('>> checking p values for {} candidates ||'.format(len(candidates)))
        start_time = time.time()
        p_values_map = soft_fd(candidates, columns, input_rdd)  # [((('A'), 'B'), 0.97)]
        print(">> Runtime: {}".format(time.time() - start_time))
        print('>> p values map for iter {}: {} ||'.format(depth, p_values_map))

        # Build min_fd and min_soft_fd lists
        for (lhs, rhs), p_val in p_values_map:
            if p_val == 1:  # case 1: hard functional dependency
                min_fd[rhs].append((lhs, rhs))
            elif p_val >= TAU:  # case 2: soft functional dependency
                if all(map(lambda x: not set(x).issubset(set(lhs)), map(lambda fd: fd[0], min_soft_fd[key]))):
                    min_soft_fd[rhs].append((lhs, rhs))

    print(">> Total runtime: {} ||".format(time.time() - initial_time))

    # print(min_fd)
    # print(min_soft_fd)


if __name__ == '__main__':
    main()

# MAY BE USEFUL
# TAU = 0.98
# # %%
# def spark_func(depth):
#     if depth == 1:
#         return [(((5,), 1), 0.99)]
#     elif depth == 2:
#         return [(((2, 3), 1), 1)]
#     elif depth == 3:
#         return [(((30, 4, 29), 1), 0.97)]
#     else:
#         print(depth)
#     # return [(((2, 3), 1), 1), (((5,), 1), 0.99), (((30, 4, 29), 1), 0.97)]
