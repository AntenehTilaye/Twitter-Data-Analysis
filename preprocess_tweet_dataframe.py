
import pandas as pd
from textblob import TextBlob
import gensim
from gensim.utils import simple_preprocess
from gensim.parsing.preprocessing import STOPWORDS
from nltk.stem import WordNetLemmatizer, SnowballStemmer
from nltk.stem.porter import *
import numpy as np
import nltk
np.random.seed(1989)

nltk.download('wordnet')

class Preprocess_Tweet:
    """
    preprocess Tweet Data
    """
    def __init__(self, df:pd.DataFrame):
        self.df = df
        self.stemmer = SnowballStemmer("english")
        print('Automation in Action...!!!')
        
    def lemmatize_stemming(self, text):
        return self.stemmer.stem(WordNetLemmatizer().lemmatize(text, pos='v'))
    
    def preprocess_text(self, text):
        result = []
        for token in gensim.utils.simple_preprocess(text):
            if token not in gensim.parsing.preprocessing.STOPWORDS and len(token) > 3:
                result.append(self.lemmatize_stemming(token))
        return ' '.join(result)
    
    def preprocess_cleaned_text(self)->pd.DataFrame:
        self.df['clean_text'] = self.df['clean_text'].map(self.preprocess_text)
            
        return self.df