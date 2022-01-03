import requests
import time
import pandas as pd
import datetime
import numpy as np

# Get data from reddit
def get_data(url, params, iteration=20):
    
    data_list = []
    
    for _ in range(iteration):

        res = get_retry(url, params, 3)
        
        if res.status_code != 200:
            continue
        
        # convert the data
        data = res.json()
        
        posts = data['data']
        
        for post in posts:
            data_list.append(post)
        
        # get the utc for the before parameter from the last of the list
        # we can get posts data only 100, so have to request over and over
        params['before'] = posts[-1]['created_utc']
        
        # decrease the loads for the website
        time.sleep(2)
        
    return pd.DataFrame(data_list)

# if the status codes were 500, 502, 505, retry
def get_retry(url, params, retry_times):
    for t in range(retry_times + 1):
        res = requests.get(url, params)
        if t < retry_times:
            if res.status_code in [500, 502, 505]:
                time.sleep(2)
                continue
        return res

def main():
    # the data is from r/thinkpad
    url = 'https://api.pushshift.io/reddit/search/submission'
    params = {
        'subreddit': 'thinkpad',
        'size': 100,
        'before': None
    }

    data = get_data(url, params)

    # extract some columns
    extracted = data[['title', 'selftext']].copy()
    extracted.drop_duplicates(inplace=True)

    # remove comma
    extracted['title'] = extracted['title'].apply(lambda x: x.replace(',', ' ').replace('\n', ' '))
    extracted['selftext'] = extracted['selftext'].apply(lambda x: x.replace(',', ' ').replace('\n', ' '))
    
    # Add sentiment and date columns
    sentiment_random = ['neutral', 'positive', 'negative']
    date_random = [datetime.date(2021, 12, 15), datetime.date(2021, 12, 16), datetime.date(2021, 12, 17)]

    extracted = extracted.assign(sentiment=[np.random.choice(sentiment_random) for _ in range(extracted.shape[0])])
    extracted = extracted.assign(date=[np.random.choice(date_random) for _ in range(extracted.shape[0])])

    # save as .csv
    extracted.to_csv('reddit_thinkpad.csv', index=False)


if __name__ == "__main__":
    main()