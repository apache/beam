

initial = 1
backoff_multiplier = 1.5

total = 0

for i in range(5):
    total += initial
    initial *= backoff_multiplier
    if initial > 20:
        initial = 20

print(total)