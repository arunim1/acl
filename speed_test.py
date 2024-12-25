import main
import matplotlib.pyplot as plt
import numpy as np
from dotdict import DotDict as dd

out = []
for max_lines in [1000, 10000, 100000, 1000000]:
    tot = 0
    for _ in range(5):
        # average over 5 runs
        inp = dd()
        inp.test_max_lines = max_lines
        inp.enable_profiling = False
        time_taken = main.main(inp)
        tot += time_taken
    print(f"Average time taken for {max_lines} lines: {tot / 5}")
    out.append((max_lines, tot / 5))

# using `out`, determine how many lines are processed per second, plot it with a line of best fit

x, y = zip(*out)
plt.plot(x, y)
plt.xscale("log")

plt.xlabel("Number of Lines")
plt.ylabel("Time Taken (seconds)")

# plot a line of best fit
coeffs = np.polyfit(x, y, 1)
line = np.poly1d(coeffs)
plt.plot(x, line(x), color="red")

plt.title("Time Taken vs. Number of Lines")
plt.show()