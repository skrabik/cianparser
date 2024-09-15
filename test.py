from sys import getsizeof

a = ['123412341234' for i in range(100_000_000)]

print(getsizeof(a) // (1024 * 1024))