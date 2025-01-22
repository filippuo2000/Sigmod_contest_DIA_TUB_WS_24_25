import re
import time

from collections import defaultdict
from src.app import (
    StartQuery,
    EndQuery,
    MatchDocument,
    GenNextAvailableRes,
)

big_file = "/Users/Filip/Downloads/big_test.txt"
small_file = "/Users/Filip/Downloads/tub_24/DIA/small_test.txt"
def main():
    with open(big_file) as f:
        # with open("/Users/Filip/Downloads/testt.txt") as f:
        num_cur_results: int = 0
        # cur_results = defaultdict(list)
        cur_results = {}
        while 1:
            try:
                line = next(f)
                ch = re.search("^.", line).group()

                if ch == 's' and num_cur_results > 0:
                    # iterating over different documents and their corresponding results
                    for id, res in cur_results.items():
                        error_flag = False
                        num_results, query_ids = GenNextAvailableRes(id)

                        if num_results != len(res):
                            error_flag = True

                        if query_ids != res:
                            error_flag = True

                        if error_flag:
                            raise ValueError(
                                f"Results for document id {id} do not match,\n \
                                                Num of true results: {len(res)}\n \
                                                Num of predicted results is: {num_results}\n \
                                                True results are: {res}\n \
                                                Predicted results are: {query_ids}"
                            )
                    num_cur_results = 0
                    cur_results.clear()

                if ch == 's':
                    n = re.findall(r"\d{1,}", line)
                    query_id, dist_type, tolerance, num_words = list(
                        map(int, n)
                    )
                    keywords = line.split()[-int(n[-1]) :]
                    assert keywords is not None
                    StartQuery(query_id, dist_type, keywords, tolerance)
                    # print("query id: ", query_id, "dist type: ", dist_type, "tolerance: ", tolerance, "num word in query: ", num_words)

                if ch == 'm':
                    # print(get_queries())
                    n = re.findall(r"\d{1,}", line)
                    doc_id, num_words = int(n[0]), int(n[1])
                    words = frozenset(line.split()[3:])
                    MatchDocument(doc_id, words)
                    # print("doc id: ", doc_id, "num of words: ", num_words)

                if ch == 'r':
                    n = re.findall(r"\d{1,}", line)
                    res_id, _, query_ids = int(n[0]), int(n[1]), n[2:]
                    query_ids = list(map(int, query_ids))
                    num_cur_results += 1
                    cur_results[res_id] = sorted(query_ids)
                    # match the document when all the results come
                    # print("result id: ", res_id, "matched queries: ", query_id)

                if ch == 'e':
                    del_query_id = int(re.search(r"\d{1,}", line).group())
                    EndQuery(del_query_id)
                    # print("id to remove: ", int(del_query_id))

                if ch is None:
                    raise ValueError(
                        f"char value {ch} is not expexted at the beginning of the line"
                    )

            except StopIteration:
                print("Reached end of file")
                break


if __name__ == "__main__":
    start = time.time()
    main()
    end = time.time()
    print(f"Elapsed running time is: {abs(end-start)}")
