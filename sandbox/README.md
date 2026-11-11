# sandbox

Small experiments I did before building the actual project.

- `queue.py` - a queue is basically a `deque`, append on one side and popleft
  on the other. A broker does the same thing, plus durability and tracking.
- `blocking.py` - consumer waits on an empty queue and wakes up when something
  arrives. `await queue.get()` is the same idea as redis `BRPOP`, which is how a
  celery worker waits for jobs.
