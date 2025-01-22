from functools import cache, lru_cache

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
    
    def search_hamming(self, trie, word: str, tolerance: int):
        def dfs(node, depth, hamming_distance):
            # If we exceed the tolerance, stop exploring
            if hamming_distance > tolerance:
                return False

            # If we reach the end of the query word
            if depth == len(word):
                # Check if the current node marks the end of a word
                return node.terminal

            # Get the index of the current character
            char_index = ord(word[depth]) - ord('a')
            found = False

            # Explore all children
            for i, child in enumerate(node.children):
                if child is not None:
                    # Increment distance if the character doesn't match
                    extra_distance = 1 if i != char_index else 0
                    # Recur to the next depth
                    if dfs(child, depth + 1, hamming_distance + extra_distance):
                        found = True

            return found

        # Start the search from the root of the Trie
        return dfs(trie.root, 0, 0)
