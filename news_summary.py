import requests
from bs4 import BeautifulSoup
from transformers import pipeline
import validators
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
from transformers import MarianMTModel, MarianTokenizer

translation_model_name = "Helsinki-NLP/opus-mt-mul-en"
translation_tokenizer = MarianTokenizer.from_pretrained(translation_model_name)
translation_model = MarianMTModel.from_pretrained(translation_model_name)

summarizer = pipeline("summarization", model="sshleifer/distilbart-cnn-12-6")

classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")

# Define Categories for News Classification
NEWS_CATEGORIES = [
    "Politics", "Government", "Elections", "Diplomacy", "Military",
    "Sports", "Football", "Basketball", "Cricket", "Tennis", "Olympics",
    "Technology", "AI", "Cybersecurity", "Gadgets", "Space",
    "Business", "Economy", "Finance", "Stock Market", "Startups", "Real Estate",
    "Health", "Medicine", "COVID-19", "Mental Health", "Nutrition", "Fitness",
    "Entertainment", "Movies", "Music", "Celebrities", "TV Shows", "Gaming",
    "Science", "Physics", "Biology", "Astronomy", "Genetics",
    "World", "Geopolitics", "International Relations", "Conflict", "War",
    "Crime", "Law", "Justice", "Police", "Terrorism", "Cybercrime",
    "Environment", "Climate Change", "Wildlife", "Sustainability", "Energy",
    "Education", "Higher Education", "Online Learning", "Research", "Universities",
    "Lifestyle", "Travel", "Fashion", "Food", "Personal Finance", "Parenting",
    "Weather", "Natural Disasters", "Hurricanes", "Floods", "Earthquakes"
]

# ScraperAPI Key
#SCRAPER_API_KEY = "8ad26a3539bc8b7d3de9d33bccdebbfa"

'''def extract_text_from_url(url):
    """Fetch and extract text using ScraperAPI (bypasses 403 errors)."""
    if not validators.url(url):  # Validate URL
        print(f"Invalid URL: {url}")
        return None
    
    api_url = f"https://api.scraperapi.com?api_key={SCRAPER_API_KEY}&url={url}"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36"
    }
    
    try:
        response = requests.get(api_url, headers=headers)
        response.raise_for_status()  # Ensure request was successful
        
        soup = BeautifulSoup(response.text, "html.parser")

        # Extract text from <p> tags
        paragraphs = soup.find_all("p")
        article_text = "\n".join([p.get_text() for p in paragraphs])

        if not article_text.strip():
            return "Content extraction failed."

        return article_text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching article: {e}")
        return "Failed to fetch the article."
'''
def translate_to_english(text):
    """Translate text from any language to English."""
    if not text or len(text.strip()) == 0:
        return "No content to translate."

    try:
        tokens = translation_tokenizer.prepare_seq2seq_batch([text], return_tensors="pt", padding=True)
        translation = translation_model.generate(**tokens)
        translated_text = translation_tokenizer.batch_decode(translation, skip_special_tokens=True)[0]
        return translated_text
    except Exception as e:
        print(f"Translation failed: {e}")
        return "Translation unavailable."

def summarize_article(text):
    """Summarize the extracted article text aiming for 50–80 words."""
    if not text or len(text) < 100:
        return "Failed to extract content from the given URL."

    text = text[:1500]  # Slightly reduced input size to boost speed

    try:
        summary = summarizer(text, max_length=80, min_length=60, do_sample=False)
        return summary[0]['summary_text']
    except Exception as e:
        print(f"Summarization failed: {e}")
        return "Summary unavailable."



def classify_news(text):
    """Classify the news article into a category."""
    classification = classifier(text, candidate_labels=NEWS_CATEGORIES, multi_label=False)
    return classification['labels'][0]  # Return the top 

