# Few Large Language Models
1) Gemini 1.5 Flash/Pro--Google
2) GPT
3) XLM
4) T5
5) Llama (3.70B)--Meta
6) Mistral --7B or 8x 7B Instruct--Mistral AI
7) Falcon
8) Titan Text G1-Express/Lite--Amazon
9) Claude instant/3.5 Sonnet/2.1 --Anthropic

# Gen AI-[End-To-End Pipeline]
- Gen AI pipeline is a set of steps followed to build an end to end genai s/w
 - Steps to follow
    - Data Acquisistion
    - Data Preparation
    - Feature Engineering--text to vector
    - Modeling
    - Evaluation
    - Deployment
    - Monitoring and model updating

## Data Aquisition
- Available data gather(csv,pdf,txt,docs,xlsx etc)
- Other data(scraping,db,internet,api etc)
- No Data-(create your own data--using LLM) via open AI gpt model to generate the data.
    Note: if you less data then we need to perform the data augmentation
    Best Augmentation techniquies
    1) Replace with synonyms (example -iam data scientise --> iam a AI Engineer)
       Image augmentation
    2) Biagram Flip ( ex-- iam ravi -->ravi is my name)
    3) Back  Translation(convert-eng to some other-telugu--> other-spanish --convert back to original-English)
    4) Add addition data or noise (ex iam a data scientist,i love this job)

## Data Preparation
    - cleanup [html, emoji's,spelling correction etc]
    - basic pre-processing
      - Tokenization|--sentence level ["iam","ravi"]
                    |--word level ["iam ravi"]
    - advance pre-processing
      - parts of speech tagging -POS tagging--noun-pronoun etc
      - parsing --
      - coreference resolution-- language expert need to help--like ravi and he in sentence
    - optional prep-processing
      - stop word removal
      - streaming--les used reduce the dimension like play,played,playing-->play--sports
      - lamatization --more used--same as above--root shud be readable
      - punctuation detection
      - language detection
      - lower case
## Feature Engineering--text to vector data
 - Text vectorization
   -  TFIDF
   -  Bag of word
   -  word2vec
   -  onehot
   -  Transformmer model

## Modelling
 - Choose Different model 
- open source llm
- paid model

## Model Evaluation
 - Intrinsive--metrics to to evaluate--gen ai engineer
 - Extrinsive --deployment

## Deployment
 - Monitoring and Re-training


## Common Terms
-  corpus --entrie text
- vocubulary -- uniquie word
- Documents --no of line --each line is a document like d1,d2
- Word--single word


