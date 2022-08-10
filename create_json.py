import json


with open('tweet_sample.json', 'w') as f:
    i = 0
    for tweets in open('data/africa_twitter_data.json'):
        print(tweets)
        json.dump(json.loads(tweets), f, indent=0)
        i = i + 1
        
        if(i == 10):
            break
    print("New json file is created from tweet_sample.json file")