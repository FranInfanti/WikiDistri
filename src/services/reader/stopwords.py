from nltk.corpus import stopwords

STOPWORDS = {
    "en": set(stopwords.words("english")),
    "fr": set(stopwords.words("french")),
}