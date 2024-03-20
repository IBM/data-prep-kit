# Document Quality Annotations
The current folder is a refactored implementation of document quality metrics. This release consists of eight metrics, 
including the criteria described in [Deepmind's Gopher paper](https://arxiv.org/pdf/2112.11446.pdf) and the [perplexity
estimation implementation by Kenneth Heafield](https://github.com/kpu/kenlm). 

## Code references
### Metrics descriptions
- Gopher (Deepmind): filter docs that
    + do not contain between 50 and 100,000 words
    + mean word length is outside the range of 3 to 10 characters; 
    + symbol-to-word ratio > 0.1 for either the hash symbol or the ellipsis; 
    + \> 90% of lines starting with a bullet point, 
    + \> 30% ending with an ellipsis. 
    + Require that 80% of words in a document contain at least one alphabetic character, 
    and apply a "stop word" filter, to remove documents that do NOT contain at least TWO of the following English words: 
    the, be, to, of, and, that, have, with; this adequately deals with ostensibly English documents 
    that contain no coherent English text.


- Perplexity score (KenLM+sp) suggested in Gopher
The smaller the perplexity score, the closer is the text to the targeted
domain (i.e., en Wikipedia). Journalistic and well written content. Distribution of perplexity for different languages may have different
shapes.

## Building the Docker image

To build the Docker image, it assumes that the model-related files (`en.arpa.bin` and `en.sp.model`) are present in a
subdirectory named `lm_sp/` However, due to their large size, these files are not included in the Git repository.

To include the `lm_sp/` directory with the necessary files, you can create the directory locally and place the files inside it.

When you run the Docker image as a container, you have the option to specify the location of the model directory by using
 the `--kenlm_model` parameter when executing the `doc_annotation_driver.py` script. The default location for the model directory is set to `/Docfilter/lm_sp/`.
