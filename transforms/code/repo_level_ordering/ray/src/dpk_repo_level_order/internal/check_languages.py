import os
import random


def guess_languages_from_special_files(df, title_column="title"):
    special_files_mapping = {
        "setup.py": ["Python"],
        "requirements.txt": ["Python"],
        "build.gradle": ["Java", "Groovy", "Kotlin", "Scala"],
        "package.json": ["JavaScript", "TypeScript"],
        "go.mod": ["Go"],
        "Cargo.toml": ["Rust"],
        "composer.json": ["Ruby"],
        "cabal.json": ["Haskell"],
        "Rakefile": ["Ruby"],
        "GemFile": ["Ruby"],
        ".sln": ["C#"],
        ".csproj": ["C#"],
        "build.sbt": ["Scala"],
        "pom.xml": ["Java"],
    }

    def is_special_file(path):
        try:
            special_files_mapping[os.path.basename(path)]
            return True
        except KeyError:
            return False

    all_files = df[title_column].to_list()
    special_files = list(filter(is_special_file, all_files))

    pro = list(map(lambda x: special_files_mapping[os.path.basename(x)], special_files))

    probable_languages = set([item for y in pro for item in y])
    return probable_languages


def get_dominant_language_repo_packing(table, language_column="language", title_column="title"):
    """
    This function takes a table with columns ['title'] and ['language'] where title
    is a path and language represents a programming language.
    """
    df = table.to_pandas()
    lan = df[language_column].value_counts()

    top_languages_info = lan.nlargest(3)
    top_languages = top_languages_info.index.values
    most_occuring = lan.idxmax()

    if len(top_languages) == 1:
        # case 1: single most popular language, choose most popular
        return most_occuring

    second_most_occuring = top_languages[1]
    print(f"Most promiment languages:  [{most_occuring} ,{second_most_occuring}]")

    # two languages are equally prominent
    if top_languages_info[most_occuring] == top_languages_info[second_most_occuring]:
        # multiple languages
        # case 2: equally popular, check probable language, choose probable language
        # detect using special files
        languages_guessed_from_special_files = guess_languages_from_special_files(df, title_column)
        lang_set = set([most_occuring, second_most_occuring])
        chosen_lang_set = lang_set.intersection(languages_guessed_from_special_files)
        if len(chosen_lang_set) > 0:
            # when there is a language common in top_languages and
            # languages_guessed_from_special_files, the order in which it appears in
            # top_languages is used for choosing the language
            chosen_lang = chosen_lang_set.pop()
            return chosen_lang

        # since we are unable to decide which language to choose, randomly choose one
        chosen_language = random.choice([most_occuring, second_most_occuring])
        print(
            f"Not much info is available to decide between {most_occuring} and {second_most_occuring}. So choosing {chosen_language}"
        )
        return chosen_language

    print(f"returning from the end of function. chosen language: {most_occuring}")
    return most_occuring
