from collections import deque

# ticket counter line
counter_line = deque()

# log aate gae line me
counter_line.append("Rahul")
counter_line.append("Priya")
counter_line.append("Amit")

print(counter_line)

# counter wala ek ek ko serve karta hai
served = counter_line.popleft()

print(f"{served} ko ticket mili")


print(counter_line)
