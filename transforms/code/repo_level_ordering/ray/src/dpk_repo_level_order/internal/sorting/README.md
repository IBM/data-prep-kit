## Sorting Module

This module supports sorting the pandas dataframes by files. It can sort files in lexical 
or semantic order.

It uses [emerge-viz](https://pypi.org/project/emerge-viz/) for source code analysis and [networkx](https://pypi.org/project/networkx/) for
building network.

Used like:

```
  from dpk_repo_level_order.internal.sorting.semantic_ordering import (
    sort_by_path,
    sort_sem,
    check_and_update_title,
)
```

Types of sorting:

They require `title` field. 

There is a `check_and_update_title` utility function to normalise title for usage with these sorting functions. 


1. Lexical sorting of paths: `sort_by_path`

```python

 df: pd.DataFrame
 sorted_df=sort_by_path(df=df, 
              logger=logger, 
              title_column_name=title_column_name)

```



2. Semantic sorting: `sort_sem` 

```python

 df: pd.DataFrame

 check_and_update_title(df)
 sorted_df = sort_sem(df=df, 
           logger=logger, 
           title_column_name=title_column_name)
  
```




