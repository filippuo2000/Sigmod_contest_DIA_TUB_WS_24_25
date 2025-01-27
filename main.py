import argparse
import re
import os
import time
import matplotlib.pyplot as plt
import numpy as np
from src.basic import ErrorCode

from src.app import PubSubVersion
from src.basic import BasicVersion


def make_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset_path', type=str)
    parser.add_argument('--basic_version', 
                        action="store_true", 
                        help="this flag activates the basic, non-optimized version")
    parser.add_argument('--output_summary_path', type=str, default="./results/summary.txt")
    args = parser.parse_args()
    return args

# big "/Users/Filip/Downloads/big_test.txt"
# small "/Users/Filip/Downloads/tub_24/DIA/small_test.txt"

# prompt:
# /Users/Filip/opt/anaconda3/envs/dia/bin/python /Users/Filip/Downloads/tub_24/DIA_my_impl/main.py --dataset_path "/Users/Filip/Downloads/tub_24/DIA/small_test.txt"
def main(args):
    doc_counter = 0
    correct_matches = 0
    wrong_matches = 0
    wrong_documents = []
    flags = {ErrorCode.EC_SUCCESS: 0, ErrorCode.EC_FAIL: 0, ErrorCode.EC_NO_AVAIL_RES: 0}

    test_file = args.dataset_path

    if args.basic_version:
        version = BasicVersion
    else:
        version = PubSubVersion

    start = time.time()
    with open(test_file) as f:
        num_cur_results: int = 0
        cur_results = {}

        while 1:
            try:
                line = next(f)
                ch = re.search("^.", line).group()

                if (ch == 's' or ch =='e') and num_cur_results > 0:
                    # iterating over different documents and their corresponding results
                    for id, res in cur_results.items():
                        results_match = False
                        query_ids = []
                        error_flag = version.GenNextAvailableRes(id, query_ids)
                        if error_flag==ErrorCode.EC_NO_AVAIL_RES :
                            print("The call to GetNextAvailRes() returned EC_NO_AVAIL_RES, but there is still undelivered documents.\n");
                            flags[ErrorCode.EC_NO_AVAIL_RES] += 1
                            return
                        if error_flag==ErrorCode.EC_SUCCESS:
                            flags[ErrorCode.EC_SUCCESS] += 1
    
                        if len(query_ids) != len(res):
                            results_match = True

                        if query_ids != res:
                            results_match = True

                        if results_match:
                            wrong_matches += 1
                            wrong_documents.append(id)
                            raise ValueError(
                                f"Results for document id {id} do not match,\n \
                                                Num of true results: {len(res)}\n \
                                                Num of predicted results is: {len(query_ids)}\n \
                                                True results are: {res}\n \
                                                Predicted results are: {query_ids}"
                            )
                        correct_matches += 1

                    num_cur_results = 0
                    cur_results.clear()

                if ch == 's':
                    n = re.findall(r"\d{1,}", line)
                    query_id, dist_type, tolerance, num_words = list(
                        map(int, n)
                    )
                    keywords = line.split()[-int(n[-1]):]
                    assert keywords is not None
                    error_flag = version.StartQuery(query_id, dist_type, keywords, tolerance)
                    if error_flag==ErrorCode.EC_FAIL:
                        print("The call to StartQuery() returned EC_FAIL.\n");
                        flags[ErrorCode.EC_FAIL] += 1
                        return
                    elif error_flag==ErrorCode.EC_SUCCESS:
                        flags[ErrorCode.EC_SUCCESS] += 1


                if ch == 'm':
                    n = re.findall(r"\d{1,}", line)
                    doc_id, _ = int(n[0]), int(n[1])
                    words = line.split()[3:]
                    error_flag = version.MatchDocument(doc_id, words)
                    if error_flag==ErrorCode.EC_FAIL:
                        print("The call to StartQuery() returned EC_FAIL.\n");
                        flags[ErrorCode.EC_FAIL] += 1
                        return
                    elif error_flag==ErrorCode.EC_SUCCESS:
                        flags[ErrorCode.EC_SUCCESS] += 1
                    doc_counter += 1

                if ch == 'r':
                    n = re.findall(r"\d{1,}", line)
                    res_id, _, query_ids = int(n[0]), int(n[1]), n[2:]
                    query_ids = list(map(int, query_ids))
                    num_cur_results += 1
                    cur_results[res_id] = sorted(query_ids)

                if ch == 'e':
                    del_query_id = int(re.search(r"\d{1,}", line).group())
                    error_flag = version.EndQuery(del_query_id)
                    if error_flag==ErrorCode.EC_FAIL:
                        print("The call to StartQuery() returned EC_FAIL.\n");
                        flags[ErrorCode.EC_FAIL] += 1
                        return
                    elif error_flag==ErrorCode.EC_SUCCESS:
                        flags[ErrorCode.EC_SUCCESS] += 1

                if ch is None:
                    raise ValueError(
                        f"char value {ch} is not expected at the beginning of the line"
                    )

            except StopIteration:
                for id, res in cur_results.items():
                    results_match = False
                    query_ids = []
                    error_flag = version.GenNextAvailableRes(id, query_ids)

                    if error_flag==ErrorCode.EC_NO_AVAIL_RES :
                        print("The call to GetNextAvailRes() returned EC_NO_AVAIL_RES, but there is still undelivered documents.\n");
                        flags[ErrorCode.EC_NO_AVAIL_RES] += 1
                        return
                    if error_flag==ErrorCode.EC_SUCCESS:
                        flags[ErrorCode.EC_SUCCESS] += 1

                    if len(query_ids) != len(res):
                        results_match = True

                    if query_ids != res:
                        results_match = True

                    if results_match:
                        wrong_matches += 1
                        wrong_documents.append(id)
                        raise ValueError(
                            f"Results for document id {id} do not match,\n \
                                            Num of true results: {len(res)}\n \
                                            Num of predicted results is: {len(query_ids)}\n \
                                            True results are: {res}\n \
                                            Predicted results are: {query_ids}"
                        )
                    correct_matches += 1

                num_cur_results = 0
                cur_results.clear()
                print("Reached end of file")
                break

    end = time.time()
    run_time = abs(end-start)
    print(f"Elapsed running time is: {run_time}")
    f = open("/Users/Filip/Downloads/full_run_basic_small.txt", "w")
    # f = open(args.output_summary_path, "w")
    f.writelines([f"RUN ON {test_file.split('/')[-1]}\n",
                    f"NUM OF DOCUMENTS: {doc_counter}\n",
                    f"NUM CORRECT MATCHES: {correct_matches}\n", 
                    f"NUM WRONG MATCHES: {wrong_matches}\n",
                    f"WRONG MATCHES: {wrong_documents}\n", 
                    f"RUN TIME: {run_time}\n",
                    f"FLAGS: {flags}\n"])
    f.close()
    # reset_everything()
    return run_time


if __name__ == "__main__":
    args = make_parse()
    main(args)
    # runtimes = []
    # for i in range(5):
    #     run_time = main()
    #     runtimes.append(run_time)
    # mean = np.mean(runtimes)
    # plt.boxplot(runtimes)
    # # Add a red marker for the mean
    # plt.scatter([1], [mean], color='red', zorder=5, label="Mean")
    # plt.text(1.2, mean, f'Mean: {mean:.2f}', color='red', fontsize=12, va='center', ha='left')
    # plt.xlabel("SPEEDUP results for 5 runs on small dataset")
    # plt.ylabel("time [s]")
    # plt.legend()
    # plt.grid()
    # # plt.savefig("/Users/Filip/Downloads/res/speedup_small.png")
    # plt.show()
    
