#%%
import itertools
cols = range(45)
#cols = ['A', 'B', 'C', 'D', 'E']
min_fd = {}
for col in cols:
    min_fd[col] = []
# min_fd['A'] = [(('B'), 'A')]
# min_fd['B'] = [(('A', 'C'), 'B')]
# min_fd[1] = [((2, 3), 1), ((5,), 1), ((30, 4, 29), 1)]

# %%
min_soft_fd = {}
for col in cols:
    min_soft_fd[col] = []

# candidates = {}

TAU = 0.98
#%%

for depth in range(1, 4):
    perms = {}
    # Create permutations for current layer
    for col in cols:
        perms[col] = [(left, col) for left in itertools.combinations([c for c in cols if c != col], depth)]
    candidates = []
    for key, partition in perms.items():
        for candidate in partition:
                   
            if all(map(lambda x: not set(x).issubset(set(candidate[0])), map(lambda fd: fd[0], min_fd[key]))):
                candidates.append(depth) # candidate

    # TODO: ask spark for FD's
    p_values_map = spark_func(depth) # [((('A',), 'B'), 0.97)]

    # Build min_fd and min_soft_fd lists
    for (lhs, rhs), p_val in p_values_map:
        if p_val == 1: # case 1: hard functional dependency
            min_fd[rhs].append((lhs, rhs))
        elif p_val >= TAU:  # case 2: soft functional dependency
            if all(map(lambda x: not set(x).issubset(set(lhs)), map(lambda fd: fd[0], min_soft_fd[key]))):
                min_soft_fd[rhs].append((lhs, rhs))
print(min_fd)
print(min_soft_fd)
# %% test driven development (TDD) testkees
import random

def spark_func(depth):
    if depth == 1:
        return [(((5,), 1), 0.99)]
    elif depth == 2:
        return [(((2, 3), 1), 1)]
    elif depth == 3:
        return [(((30, 4, 29), 1), 0.97)]
    else:
        print(depth)
    # return [(((2, 3), 1), 1), (((5,), 1), 0.99), (((30, 4, 29), 1), 0.97)]

        