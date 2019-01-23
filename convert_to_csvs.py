# -*- coding: utf-8 -*-
"""
The following code was produced as part of a project sponsored by the Department of the Navy,
Office of Naval Research under ONR Grant No. N00014-18-1-2128.

Copyright 2018 The Johns Hopkins University Applied Physics Laboratory LLC

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS
BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

import json
import csv
import os
import argparse
import pandas as pd

"""
Script converts files containing line-by-line jsons into csvs.

*Input parameters
- source. Source directory
- target. Target directory
- start. First date to parse (if exists). Date format: "YYYY-MM-DD"
- end. Last dated to parse (if exists). Date format: "YYYY-MM-DD"
- keywordfile. Path to a file containing keywords.  These are expected to be listed line-by-line.
- keywordflag. Name for column containing True/False based on whether tweet contains keywords.
- keywordfields. Fields to check in tweet for keywords.  Options: author, text, entities. Expected to be comma-separated. If absent, will check all fields.
- dofilter. If this is present, will only write tweets containing a keyword.

* Source directory structure:
Source dir contains subdirectories specific to language.
Within these subdirectories, each month is separated out via the folder name "YYYY-MM".
Each month subdirectory contains files labeled by day "DD" (see INPUT_FORMAT).

* Target directory structure:
Target dir contains subdirectories specific to language.
Each subdirectory will contain csv files labeled by date (see OUTPUT_FORMAT).
"""

LANGUAGES = ["ar", "de", "en", "fr", "ru"]
KEYWORD_FIELDS = ["text", "author", "entities"]
INPUT_FORMAT = "{folder}/{l}/{y}_{m:02}/{d:02}.txt"
OUTPUT_FORMAT = "{folder}/{l}/{y}_{m:02}_{d:02}" # suffix added later

# Note: replacing truncated fields with their full content.
BASIC_FIELDS = ['timestamp_ms', 'in_reply_to_screen_name', 'in_reply_to_user_id_str', 'favorite_count',
          'quote_count', 'coordinates', 'favorited', 'id_str', 'text', 'in_reply_to_user_id', 'place', 'geo',
          'retweet_count', 'reply_count', 'in_reply_to_status_id', 'retweeted', 'is_quote_status', 'filter_level', 'lang',
          'in_reply_to_status_id_str', 'created_at', 'source', 'contributors']

USER_FIELDS = ['statuses_count', 'utc_offset', 'listed_count', 'translator_type', 'followers_count', 'time_zone', 'name', 'protected',
               'screen_name', 'is_translator', 'contributors_enabled', 'url', 'id_str', 'location', 'created_at', 'geo_enabled', 'description',
               'lang', 'profile_image_url', 'profile_background_image_url', 'favourites_count', 'verified', 'friends_count']
ENTITY_FIELDS = ["urls", "user_id_mentions", "user_screenname_mentions", "symbols", "hashtags", "media"]

USER_FIELDS_OUT = ["user.{f}".format(f=field) for field in USER_FIELDS]

# Note: replacing 'retweeted_status' and 'quoted_status' with the respective status ids.  Recording those tweets separately.
TWEET_REF_FIELDS = ["retweeted_status", "quoted_status"]

FIELDNAMES = BASIC_FIELDS + TWEET_REF_FIELDS + ENTITY_FIELDS + ["user.id_str"]

# Filtering functions

class KeywordFilter:
    
    @classmethod
    def from_file(cls, keywordfile, flag, do_filter, filters):
        keywords = []
        with open(keywordfile, 'r') as keyfile:
            for line in keyfile.readlines():
                keywords.append(line.strip())
        return cls(keywords, flag, do_filter, filters)

    def __init__(self, keywords, flag, do_filter, filters = None):
        self.keywords = keywords
        self.flag = flag
        self.do_filter = do_filter
        FILTERS = {"author": self.check_author, "text": self.check_text, "entities": self.check_entities}
        if filters:
            self.filters = {f: FILTERS[f] for f in FILTERS if f in filters}
        else:
            self.filters = FILTERS

    def check_author(self, tweet):
        author = tweet['user']['screen_name']
        # exact equals, is that good?
        return any(term.replace(" ", "") == author for term in self.keywords)

    def check_entities(self, tweet):
        hashtags = []
        mentions = []
        entities = tweet.get('entities')
        if entities:
            hashtags = [h['text'].lower() for h in entities.get("hashtags")]
            mentions = [m['screen_name'] for m in entities.get("user_mentions")]
        if tweet['truncated']:
            extended_entities1 = tweet['extended_tweet'].get('entities')
            if extended_entities1:
                hashtags1 = extended_entities1.get("hashtags")
                mentions1 = extended_entities1.get("user_mentions")
                if hashtags1:   hashtags.extend([h['text'].lower() for h in hashtags1])
                if mentions1:   mentions.extend([m['screen_name'] for m in mentions1])
            extended_entities2 = tweet['extended_tweet'].get("extended_entities")
            if extended_entities2:
                hashtags2 = extended_entities2.get("hashtags")
                mentions2 = extended_entities2.get("user_mentions")
                if hashtags2:   hashtags.extend([h['text'].lower() for h in hashtags2])
                if mentions2:   mentions.extend([m['screen_name'] for m in mentions2])
        vals = hashtags + mentions
        return not set(vals).isdisjoint(self.keywords)

    def check_text(self, tweet):
        if tweet['truncated']:
            text = tweet['extended_tweet']['full_text']
        else:
            text = tweet['text']
        text = text.lower()
        return any(term in text for term in self.keywords)

    def check_tweet(self, tweet):
        filter_results = [ self.filters[field](tweet) for field in self.filters.keys()]
        tweet_result = any( filter_results )
        return tweet_result

    def decide_write(self, tweet):
        has_keyword = self.check_tweet(tweet)
        return (self.keywords is None) or (self.do_filter == False) or (self.do_filter and has_keyword)



# Parsing functions

def parse_entity_details(entities):
    record = { field : [] for field in ENTITY_FIELDS }
    if entities is not None:
        urls = entities.get("urls")
        url_links = []
        if urls is not None and len(urls) > 0:
            url_links = [u['expanded_url'] for u in urls]
        record['urls'] = url_links
        media = entities.get("media")
        media_links = []
        if media is not None and len(media) > 0:
            media_links = [m['media_url'] for m in media]
        record['media'] = media_links
        hashtags = entities.get("hashtags")
        if hashtags is not None and len(hashtags) > 0:
            record['hashtags'] = [h['text'] for h in hashtags]
        mentions = entities.get("user_mentions")
        if mentions is not None and len(mentions) > 0:
            record["user_id_mentions"] = [m['id_str'] for m in mentions]
            record["user_screenname_mentions"] = [m["screen_name"] for m in mentions]
        symbols = entities.get("symbols")
        if symbols is not None and len(symbols) > 0:
            record["symbols"] = [s["text"] for s in symbols]
    return record


def extract_entities(tweet):
    entities = tweet.get("entities")
    parsed_entities = parse_entity_details(entities)
    if tweet.get("truncated") is True:
        extended = tweet.get("extended_tweet")
        entities2 = extended.get("entities")
        if entities2 is not None:
            parsed_entities2 = parse_entity_details(entities2)
            for field in ENTITY_FIELDS:
                parsed_entities[field].extend(parsed_entities2[field])
        ext_entities = extended.get("extended_entities")
        if ext_entities is not None:
            parsed_ext_entities = parse_entity_details(ext_entities)
            for field in ENTITY_FIELDS:
                parsed_entities[field].extend(parsed_ext_entities[field])
    return parsed_entities


def record_user(tweet):
    user = tweet.get("user")
    user_record = { field: None for field in USER_FIELDS_OUT }
    user_record["tweet.created_at"] = tweet.get("created_at")
    for field in USER_FIELDS:
        user_record["user.{f}".format(f=field)] = user.get(field)
    return user_record


def record_tweet(tweet):
    record = { field: None for field in FIELDNAMES }
    record_refs = []
    for field in BASIC_FIELDS:
        record[field] = tweet.get(field)
    if tweet.get("truncated") is True:
        extended = tweet.get("extended_tweet")
        record['text'] = extended.get("full_text")
    # extract entities
    entities = extract_entities(tweet)
    for field in ENTITY_FIELDS:
        record[field] = entities[field]
    # handle references to retweeted or quoted tweets
    for field in TWEET_REF_FIELDS:
        ref = tweet.get(field)
        if ref is not None:
            record[field] = ref.get("id_str")
            record_refs.append(ref)
    return (record, record_refs)


# Writing functions
                          
def write_record_to_file(record, output_filename, is_user, keywordflag = None):
    if is_user:
        output_filename = output_filename + "_users.csv"
        fieldnames = USER_FIELDS_OUT
    else:
        output_filename = output_filename + ".csv"
        fieldnames = FIELDNAMES
        if keywordflag:
            fieldnames = list(fieldnames) + [keywordflag]
    write_header = os.path.isfile(output_filename) is False # if file is new, plan to add header
    with open(output_filename, 'a') as outfile:
        writer = csv.DictWriter(outfile, fieldnames, extrasaction='ignore')
        if write_header:  writer.writeheader()
        writer.writerow(record)


def process_tweet(tweet, output_file, keywordfilter):
    if keywordfilter:
        write_record = keywordfilter.decide_write(tweet)
        keyword_flag = keywordfilter.flag
    else:
        write_record = True
        keyword_flag = None
    if write_record:
        (record, refs) = record_tweet(tweet)
        if keyword_flag:
            record[keyword_flag] = keywordfilter.check_tweet(tweet)
        user_record = record_user(tweet)
        record['user.id_str'] = user_record["user.id_str"]
        write_record_to_file(record, output_file, False, keyword_flag)
        write_record_to_file(user_record, output_file, True, keyword_flag)
        # parse referenced tweets
        for ref in refs:
            process_tweet(ref, output_file, keywordfilter)


def deduplicate_file(filename):
    df = pd.read_csv(filename, dtype=object)
    df = df.drop_duplicates()
    df.to_csv(filename, index=False, float_format='%f')


def process_file(filename, output_filename, keywordfilter = None):
    with open(filename, 'r') as tweet_file:
        for line in tweet_file.readlines():
            try:
                tweet = json.loads(line)
                process_tweet(tweet, output_filename, keywordfilter)
            except Exception as e:
                print("Unable to load tweet object")
                print(e)
    output_tweets = output_filename + ".csv"
    if os.path.isfile(output_tweets):
        deduplicate_file(output_tweets)
    output_users = output_filename + "_users.csv"
    if os.path.isfile(output_users):
        deduplicate_file(output_users)
    
    
def main(source_dir, target_dir, date_range, keywordfilter = None):
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
    
    print("==Processing Files==")
    for lang in LANGUAGES:
        print("Language: {0}".format(lang))
        lang_dir = "{0}/{1}".format(target_dir, lang)
        if not os.path.isdir(lang_dir):
            os.makedirs(lang_dir)
        for date in date_range:
            input_filename = INPUT_FORMAT.format(folder=source_dir,
                                             l=lang,
                                            y=date.year,
                                            m=date.month,
                                            d=date.day)
            if os.path.isfile(input_filename):
                print("{0}-{1:02}-{2:02}".format(date.year, date.month, date.day))
                output_filename = OUTPUT_FORMAT.format(folder=target_dir,
                                   l=lang,
                                   y=date.year,
                                   m=date.month,
                                   d=date.day)
                try:
                    process_file(input_filename, output_filename, keywordfilter)
                except Exception as e:
                    print("Unable to process {f}".format(f=input_filename))
                    print(e)
    print("==Done!==")

                    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Convert files containing line-by-line jsons to csvs.")
    parser.add_argument('--source', help="source directory", required = True)
    parser.add_argument('--target', help="target directory", required = True)
    parser.add_argument('--start', help="First date to parse (if exists). Date format: YYYY-MM-DD", required = True)
    parser.add_argument('--end', help="Last date to parse (if exists). Date format: YYYY-MM-DD", required = True)
    parser.add_argument('--keywordfile', help="Path for file containing keywords. Optional for filtering/flagging.", required = False, default = None)
    parser.add_argument('--keywordflag', help="String to column denoting whether tweet contains keyword.  Optional, for flagging.", required = False, default = "contains_keyword")
    parser.add_argument('--keywordfields', help="Comma-separated list of fields to check for keywords. Options: author, text, entities. Will check all by default.", required = False, default = None)
    parser.add_argument('--dofilter', help="If included, will filter to only write tweets containing keywords.", dest = 'dofilter', required = False,
                        action='store_true', default = False)
    
    args = parser.parse_args()

    dofilter = bool(args.dofilter)
    if args.keywordfields:
        keywordfields = [ field for field in args.keywordfields.split(",")]
    else:
        keywordfields = KEYWORD_FIELDS
    print("==Reading Keywords File==")
    if args.keywordfile is not None:
        keywordfilter= KeywordFilter.from_file(args.keywordfile, args.keywordflag, dofilter, keywordfields)
    else:
        keywordfilter= None

    start_date = [int(x) for x in args.start.split("-")]
    start_date = pd.datetime(start_date[0], start_date[1], start_date[2])
    end_date = [int(x) for x in args.end.split("-")]
    end_date = pd.datetime(end_date[0], end_date[1], end_date[2])
    date_range = pd.date_range(start_date, end_date)
    main(source_dir=args.source, target_dir=args.target, date_range=date_range, keywordfilter = keywordfilter)

