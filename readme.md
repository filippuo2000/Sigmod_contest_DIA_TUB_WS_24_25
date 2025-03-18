# Implementation of SIGMOD Contest 2013 - project for DIA lecture at TUB WS24/25

This repository contains the implementation for the [SIGMOD Contest 2013](https://transactional.blog/sigmod-contest/2013), which focuses on the continuous stream of queries processing task. Queries contain strings and type of a calculated distance (normal, Hamming dist, Edit dist) and are matched against documents containing strings. \n

The task was to first implement a basic implementation, which in this case was simply a brute-force solution and then refine the solution to achieve a minimum of 20x speedup on the processing of test .txt files. Speed up was achieved by implementing the publish-subscribe architecture, where a set of words from the active queries are the topics, to which certain queries subscribe. Publisher is then publishing single words from every document that need to be processed. Further speedup has been achieved by implementing the caching mechanism, so that for every document only the unprocessed topics are checked against it. 

So far only the basic implementation without any optimization involved has been designed.

To successfully run the python implementation one should use python 3.12+ and install the Levenshtein package, which is a dependency that provides the implementation of the Edit distance.

  ```sh
pip install levenshtein
   ```

Once it has been installed, the project can simply be ran from the command line via the following command,

For speedup implementation:
 ```sh
python main.py --dataset_path path_to_test.txt --output_file_path output_file.txt
 ```

For basic implementation:

  ```sh
  python main.py --dataset_path path_to_test.txt --basic_version --output_file_path output_file.txt
   ```

where:
- **--dataset_path** — required argument for specifying the path to the input test file in text format.
- **--basic_version** — a flag indicating that a basic solution should be executed. If omitted, the improved one is used.
- **--output_summary_path** — required argument specifying the path to a text file where the program’s output will be saved. This output includes:
  - A summary of returned error codes
  - The number of successfully processed documents
  - The number of mismatches where the results deviate from expected outputs

