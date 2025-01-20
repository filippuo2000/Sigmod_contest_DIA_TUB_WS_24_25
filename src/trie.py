class TrieNode:
    __slots__ = ("terminal", "children")
    def __init__(self):
        self.terminal = False
        self.children = [None]*26

class Trie:
    def __init__(self):
        self.root = TrieNode()
    def insert(self, word: str):
        curr_node = self.root
        for ch in word:
            ch_idx = ord(ch) - ord('a')
            if curr_node.children[ch_idx] is None:            
                curr_node.children[ch_idx] = TrieNode()
            curr_node = curr_node.children[ch_idx]
        curr_node.terminal = True

    def search(self, word: str):
        curr_node = self.root
        for ch in word:
            ch_idx = ord(ch) - ord('a')
            if curr_node.children[ch_idx] is None:
                return False
            else:
                curr_node = curr_node.children[ch_idx]
                
        return curr_node.terminal
    
    # def search_hamming(self, trie, word: str, tolerance: int):
    #     def dfs(node, depth, hamming_distance):
    #         # If we exceed the tolerance, stop exploring
    #         if hamming_distance > tolerance:
    #             return False

    #         # If we reach the end of the query word
    #         if depth == len(word):
    #             # Check if the current node marks the end of a word
    #             return node.terminal

    #         # Get the index of the current character
    #         char_index = ord(word[depth]) - ord('a')
    #         found = False

    #         # Explore all children
    #         for i, child in enumerate(node.children):
    #             if child is not None:
    #                 # Increment distance if the character doesn't match
    #                 extra_distance = 1 if i != char_index else 0
    #                 # Recur to the next depth
    #                 if dfs(child, depth + 1, hamming_distance + extra_distance):
    #                     found = True

    #         return found

    #     # Start the search from the root of the Trie
    #     return dfs(self.root, 0, 0)


    def search_hamming(self, word: str, tolerance: int, curr_node: TrieNode):
        def calc(word: str, curr_node: TrieNode, hamming_dist: int, depth: int):            
            if hamming_dist > tolerance:
                return False
            if len(word)==depth:
                    return curr_node.terminal

            char_rel_idx = ord(word[depth]) - ord('a')

            # ret = False
            for idx, child in enumerate(curr_node.children):
                if child is not None:
                    extra_dist = 1 if idx!=char_rel_idx else 0

                    if calc(word, child, hamming_dist+extra_dist, depth+1):
                        return True
            return False
        return calc(word, curr_node, 0, 0)
    
    def build_trie(self, words: set[str]):
        for word in words:
            self.insert(word)


# class Trie:
#     def __init__(self):
#         self.root = TrieNode()
#     def insert(self, word: str):
#         curr_node = self.root
#         for ch in word:
#             ch_idx = ord(ch) - ord('a')
#             if curr_node.children[ch_idx] is None:            
#                 curr_node.children[ch_idx] = TrieNode()
#             curr_node = curr_node.children[ch_idx]
#         curr_node.terminal = True

#     def search(self, word: str):
#         curr_node = self.root
#         for ch in word:
#             ch_idx = ord(ch) - ord('a')
#             if curr_node.children[ch_idx] is None:
#                 return False
#             else:
#                 curr_node = curr_node.children[ch_idx]
                
#         return curr_node.terminal
    
#     def search_hamming(self, word: str, tolerance: int, curr_node: TrieNode, curr_score: int, char_abs_idx: int):
#         char_rel_idx = ord(word[char_abs_idx-1]) - ord('a')
        
#         for idx, child in enumerate(curr_node.children):
#             if child is not None:
#                 extra_score = 0
#                 if idx==char_rel_idx:
#                     extra_score = 1
#                 if len(word)==char_abs_idx:
#                     return child.terminal and char_abs_idx-(curr_score+extra_score) <= tolerance
#                 if char_abs_idx==tolerance+1 and curr_score+extra_score==0:
#                     return False
#                 if char_abs_idx-(curr_score+extra_score) > tolerance:
#                     return False
#                 # if len(word)-char_abs_idx <= tolerance-(char_abs_idx-(curr_score+extra_score)):
#                 #     return True
                
#                 res = self.search_hamming(word, tolerance, curr_node.children[idx], curr_score+extra_score, char_abs_idx+1)
#                 if res:
#                     return True
#         return False
    
#     def build_trie(self, words: set[str]):
#         for word in words:
#             self.insert(word)
