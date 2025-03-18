# Implementation of SIGMOD Contest 2013 - project for DIA lecture at TUB WS24/25

This repository contains the implementation for the SIGMOD Contest 2013, which focuses on processing a large number of queries for reading words from files. \\n

So far only the basic implementation without any optimization involved has been designed.

To successfully run the python implementation one should use python 3.12+ and install (most likely via
pip), the Levenshtein package, which is a dependency that provides the implementation of the Edit distance.

  ```sh
pip install levenshtein
   ```

Once it has been installed, the project can simply be ran from the command line via the following command,
in this case with the basic implementation:

  ```sh
  python main.py --dataset_path path_to_test.txt --basic_version --output_file_path output_file.txt
   ```
where:
− **-dataset path**: a required argument for specifying the path to the input test file in text format.
− **-basic version**: a flag indicating that a basic solution should be executed. If omitted, improved one is used.
− **-output_summary_path**: a required argument specifying the path to a text file where the program’s output will
be saved. This output includes details such as a summary of returned error codes, the number of successfully
processed documents, and the number of mismatches where the results deviate from expected outputs.

For clarity, below is presented the command to run the improved version of the program:
 ```sh
python main.py --dataset_path path_to_test.txt --output_file_path output_file.txt
 ```
