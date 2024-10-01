import pyarrow.csv as pacsv
import csv



class TrieNode:
    '''
    Implements one node of a Trie datastructure
    '''
    def __init__(self):
        self.children = {}
        self.is_end_of_word = False
        self.data = None

class Trie:
    '''
    Implements a Trie datastructure for efficient retrieval of concepts from the IKB.
    '''
    def __init__(self):
        self.root = TrieNode()

    def insert(self, library_name, programming_language, functionality):
        node = self.root
        for char in library_name:
            if char not in node.children:
                node.children[char] = TrieNode()
            node = node.children[char]
        node.data = {}
        node.data['Category'] = functionality
        node.data['Language'] = programming_language
        node.is_end_of_word = True

    def search(self, library_name, programming_language):
        node = self.root
        for char in library_name:
            if char not in node.children:
                return None 
            node = node.children[char]
        if node.is_end_of_word and node.data:
            return node.data
        return None


class knowledge_base:
    '''
    Implements the internal knowledge base.
    '''
    knowledge_base_file = ''
    null_file = ''
    knowledge_base_table = None
    knowledge_base_trie = None
    entries_with_null_coverage = set()
    
    def __init__(self, ikb_file, null_libs_file):
        self.knowledge_base_file = ikb_file
        self.null_file = null_libs_file 

    def load_ikb_trie(self):
        self.knowledge_base_table = pacsv.read_csv(self.knowledge_base_file)
        self.knowledge_base_trie = Trie()
        library_column = self.knowledge_base_table.column('Library').to_pylist()
        language_column = self.knowledge_base_table.column('Language').to_pylist()
        category_column = self.knowledge_base_table.column('Category').to_pylist()
        for library, language, category in zip(library_column, language_column, category_column):
            self.knowledge_base_trie.insert(str.lower(library), language, category)

    def write_null_files(self):
        with open(self.null_file, 'a+', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            for entry in self.entries_with_null_coverage:
                writer.writerow([entry[0], entry[1]])
        self.entries_with_null_coverage = set()


def concept_extractor(libraries,language,ikb):
    '''
    Given a set of libraries and the corresponding programming language along with the IKB trie, this function
    returns the matching concept(s) as a comma separated list joined into a string.
    '''
    concept_coverage = set()
    language = language
    libraries = [item.strip() for item in libraries.split(",")]
    for library in libraries:
        if library:
            extracted_base_name = str.lower(library)
            matched_entry = ikb.knowledge_base_trie.search(extracted_base_name, language)
            if matched_entry:
                concept_coverage.add(matched_entry['Category'].strip())
            else:
                ikb.entries_with_null_coverage.add((library,language))
    return ','.join(sorted(list(concept_coverage)))