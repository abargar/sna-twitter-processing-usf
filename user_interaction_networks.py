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

import os
import sys
import argparse
import pandas as pd
import ast
import csv
"""
Script creates interaction networks from Twitter data that has been previously converted into csvs.

These interaction networks are edgelists based on one of the following types of Twitter interactions:
- replies
- retweets
- quotes
- mentions
- user-to-hashtags

* Input parameters:
- source. Source directory
- target. Target directory
- start. First date to parse (if exists). Date format: "YYYY-MM-DD"
- end. Last dated to parse (if exists). Date format: "YYYY-MM-DD"
- interactions. Nullable. Comma-separated list of interations. Will extract all interactions by default.
- languages. Nullable. Comma-separated list of languages. Will extract lal languages by default.

* Source directory structure:
Source dir contains subdirectories specific to language.
Each subdirectory contains csv files labeled by date (see source_file_format).

* Target directory structure:
A new folder called 'user_interactions" will be placed in the target directory.
Each type of interaction will be stored in a subdirectory underneath this new folder, e.g. "user_interactions/retweets"
Subdirectories and dates will be collapsed into the output file format, e.g. "ar_YYYY_MM_DD.csv"
- This is done to enable simpler file concatenation across languages in a future step.
 -- This may be done, for example, to create a network of all interactions in a single day across all languages.

"""
# Global Defaults
NETWORK_TYPES = ["replies", "retweets", "quotes", "mentions", "hashtags"]
LANGUAGES = ["ar", "de", "en", "fr", "ru"]

def extract_replies(df):
    replies = df[["created_at", "id_str", "user.id_str",
                 "in_reply_to_user_id_str", "in_reply_to_status_id_str"]]
    replies = replies.loc[replies["in_reply_to_user_id_str"].notnull()]
    replies = replies.drop_duplicates()
    return replies

def extract_retweets(df):
    retweet_refs = df[["id_str", "user.id_str"]].rename(columns={"user.id_str": "retweeted_user",
                                              "id_str": "retweeted_status"})
    retweets = df[["created_at", "id_str", "user.id_str", "retweeted_status"]]
    retweets = retweets[retweets["retweeted_status"].notnull()]
    retweets_with_refs = pd.merge(retweets, retweet_refs,
                                  on="retweeted_status",
                                  how="left")
    retweets_with_refs = retweets_with_refs.drop_duplicates()
    return retweets_with_refs   

def extract_quotes(df):
    quote_refs = df[["id_str", "user.id_str"]].rename(columns={"user.id_str": "quoted_user",
                                            "id_str": "quoted_status"})
    quotes = df[["created_at", "id_str", "user.id_str", "quoted_status"]]
    quotes = quotes[quotes["quoted_status"].notnull()]
    quotes_with_refs = pd.merge(quotes, quote_refs,
                                  on="quoted_status",
                                how="left")
    quotes_with_refs = quotes_with_refs.drop_duplicates()
    return quotes_with_refs

def extract_mentions(df):
    mentions = df[["created_at", "id_str", "user.id_str", "user_id_mentions"]]
    mentions = mentions.loc[mentions['user_id_mentions'].notnull()]
    mentions = mentions.loc[mentions['user_id_mentions'].str.len() != 2] # string representation of empty list is '[]'
    mentions['user_id_mentions'] = mentions['user_id_mentions'].apply(ast.literal_eval)
    return mentions

def extract_hashtags(df):
    hashtags = df[["created_at", "id_str", "user.id_str", "hashtags"]]
    hashtags = hashtags.loc[hashtags['hashtags'].notnull()]
    hashtags = hashtags.loc[hashtags['hashtags'].str.len() != 2] # string representation of empty list is '[]'
    hashtags['hashtags'] = hashtags['hashtags'].apply(ast.literal_eval)
    return hashtags

def write_edgelist(df, output_file_format, interaction_type, lang, date):
    output_file = output_file_format.format(
                                                    interaction=interaction_type,
                                                    l=lang,
                                                    y=date.year,
                                                    m=date.month,
                                                    d=date.day)
    df.to_csv(output_file, index=False, float_format='%f')

def write_embedded_edgelist(df, output_file_format, embedded_field, output_field, interaction_type, lang, date):
    output_file = output_file_format.format(
                                                                interaction=interaction_type,
                                                                l=lang,
                                                                y=date.year,
                                                                m=date.month,
                                                                d=date.day)
    headers = list(df.columns)
    headers.remove(embedded_field)
    headers.append(output_field)
    with open(output_file, 'w') as out:
        writer = csv.writer(out)
        writer.writerow(headers)
        for i in range(df.shape[0]):
            row = df.iloc[i]
            for f_index in range(len(row[embedded_field])):
                writer.writerow([row['created_at'], row['id_str'], row['user.id_str'], row[embedded_field][f_index]])

def create_networks(source_dir, target_dir, date_range, network_choices = NETWORK_TYPES, languages = LANGUAGES):
    print("==Creating User Interaction Networks==")
    
    # structuring basic file formats
    source_file_format = source_dir + "/{l}/{y}_{m:02}_{d:02}.csv"
    output_dir = target_dir + "/user_interactions/"
    output_file_format = output_dir + "{interaction}/{l}_{y}_{m:02}_{d:02}.csv"
    print("Output directory will be {0}".format(output_dir))

    print("==Creating Folders if Needed==")
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)
    for interaction in network_choices:
        interaction_dir = output_dir + interaction
        if not os.path.isdir(interaction_dir):
            os.makedirs(interaction_dir)
            print("Created {0}".format(interaction_dir))
        else:
            print("{0} already exists".format(interaction_dir))

    GET_RETWEETS = "retweets" in network_choices
    GET_REPLIES = "replies" in network_choices
    GET_QUOTES = "quotes" in network_choices
    GET_MENTIONS = "mentions" in network_choices
    GET_HASHTAGS = "hashtags" in network_choices

    print("==Processing Files==")
    for lang in languages:
        print("Language: {0}".format(lang))
        for date in date_range:
            source_file = source_file_format.format(l=lang, y=date.year, m=date.month, d=date.day)
            if os.path.isfile(source_file):
                print("{0}-{1}-{2}".format(date.year, date.month, date.day))
                df = pd.read_csv(source_file, lineterminator="\n", dtype=object)
                if GET_REPLIES:
                    replies = extract_replies(df)
                    write_edgelist(replies, output_file_format, "replies", lang, date)
                if GET_RETWEETS:
                    retweets = extract_retweets(df)
                    write_edgelist(retweets, output_file_format, "retweets", lang, date)
                if GET_QUOTES:
                    quotes = extract_quotes(df)
                    write_edgelist(quotes, output_file_format, "quotes", lang, date)
                if GET_MENTIONS:
                    mentions = extract_mentions(df)
                    write_embedded_edgelist(mentions, output_file_format, "user_id_mentions", "user_id.mention", "mentions", lang, date)
                if GET_HASHTAGS:
                    hashtags = extract_hashtags(df)
                    write_embedded_edgelist(hashtags, output_file_format, "hashtags", "hashtag", "hashtags", lang, date)
                    
    print("==Done!==")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Create user-user networks based on Twitter interactions.")
    parser.add_argument('--source', help="source directory", required = True)
    parser.add_argument('--target', help="target directory", required = True)
    parser.add_argument('--start', help="First date to parse (if exists). Date format: YYYY-MM-DD", required = True)
    parser.add_argument('--end', help="Last date to parse (if exists). Date format: YYYY-MM-DD", required = True)
    parser.add_argument('--types', help="Nullable. Comma-separated list of interactions. Will extract all interactions by default.", required = False)
    parser.add_argument('--languages', help="Nullable. Comma-separated list of languages. Will extract all languages by default.", required = False)
    
    args = parser.parse_args()
    
    # Assert source directory exists
    if not os.path.isdir(args.source):
        print("Error: source directory {0} not found".format(source_dir))
    else:
        if args.types:
            net_types = args.types.sep(",")
        else:
            net_types = NETWORK_TYPES
        if args.languages:
            languages = args.languages.split(",")
        else:
            languages = LANGUAGES
        try:
            # Generate date range
            start_date = [int(x) for x in args.start.split("-")]
            start_date = pd.datetime(start_date[0], start_date[1], start_date[2])
            end_date = [int(x) for x in args.end.split("-")]
            end_date = pd.datetime(end_date[0], end_date[1], end_date[2])
            date_range = pd.date_range(start_date, end_date)
        except Exception as e:
            print("Error creating date range. Given values: {0} - {1}".format(args.start, args.end))
            print(e)
        try:
            create_networks(source_dir=args.source, target_dir=args.target, date_range=date_range, network_choices=net_types, languages=languages)
        except Exception as e:
            print("Error creating networks")
            print(e)
            
