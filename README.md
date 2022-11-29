# Distributed Query Execution

In this assignment, we implement distributed query execution in a shared-nothing environment.
We start with a single-node execution (see: `coordinator.cpp`), that we want to scale to multiple workers (`worker.cpp`).

We store the data to process on external storage, and partitioned them into many small chunks, all of which will be accessible via HTTP.
Our worker processes should be an elastic compute resource, which can scale depending on the workload.
The coordinator should then be responsible to manage the workers, and additionally ensure a proper load balancing between the workers.

Before starting to write code, consider the following design questions.

## 1.Design Questions:

* How does the leader process know the number of alive workers?
* How can we distribute work "fairly" among workers?
* With what messages do leader - worker communicate?
* How can we detect failed / crashed workers?
* How do we recover when a worker fails?

## 2.Scalability Questions:

Configure the test scripts to spawn 1,2,4,8,16 workers - draw a diagram visualizing how execution time is affected by adding workers.
* What is the limit in scaling? (network, CPU-bound)
* Measure each worker’s load - how is load balancing affected by scale? 
* Could you think of a case when the coordinator would become a bottleneck in such a system?

## Execution:

Install dependencies (Ubuntu):

```bash
sudo apt install cmake g++ libcurl4-openssl-dev
```

Build executables:

```bash
mkdir -p cmake-build-debug
cd cmake-build-debug
cmake ..
make
```

Run on test data:

```bash
./runTest.sh data/test.csv
# should print 5
```

Run a larger workload with:

```bash
./testWorkload.sh https://db.in.tum.de/teaching/ws2223/clouddataprocessing/data/filelist.csv
# should print 275625
```

## Submission:
You can submit everything via GitLab.
First fork this repository, and add all members of your group to a single repository.
Then, in this group repository, add:
* Names of all members of your group in groupMembers.txt
* Code that implements the assignment
* A written report giving a brief description of your implementation, and answering the design and scalability questions.
