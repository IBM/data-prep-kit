import pyarrow as pa


table = pa.Table.from_pydict(
    {
        "contents": pa.array(
            [
                "My name is Tom chandler. Captain of the ship",
                "I work at Apple and I like to eat apples",
                "My email is tom@chadler.com and dob is 31.05.1987",
            ]
        ),
        "doc_id": pa.array(["doc1", "doc2", "doc3"]),
    }
)
expected_table = table.add_column(
    0,
    "transformed_contents",
    [
        [
            "My name is <PERSON>. Captain of the ship",
            "I work at <ORGANIZATION> and I like to eat apples",
            "My email is <EMAIL_ADDRESS> and dob is <DATE_TIME>",
        ]
    ],
)
detected_pii = [["PERSON"], ["ORGANIZATION"], ["EMAIL_ADDRESS", "DATE_TIME"]]
expected_table = expected_table.add_column(0, "detected_pii", [detected_pii])

redacted_expected_table = table.add_column(
    0,
    "transformed_contents",
    [
        ["My name is . Captain of the ship", "I work at  and I like to eat apples", "My email is  and dob is "],
    ],
)
redacted_expected_table = redacted_expected_table.add_column(0, "detected_pii", [detected_pii])
expected_metadata_list = [
    {"original_table_rows": 3, "original_column_count": 2, "transformed_table_rows": 3, "transformed_column_count": 4},
    {},
]
