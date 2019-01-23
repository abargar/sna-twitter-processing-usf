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

import pandas as pd
import argparse
import os

"""
Script creates cooccurrence networks from edgelists (stored as csv).  

* Input parameters:
- source. Source directory or file.  If directory, will parse all files and output into the target directory
with the suffix "_cooccurrences".
- target. Target directory or file.
- bytag. The column name over which the cooccurrences should be caluclated.
"""


def get_coocurrences(edgelist, by_tag):
    coocurrs = edgelist.merge(edgelist, how="inner", on=by_tag)
    coocurrs = coocurrs[coocurrs["{0}_x".format("user.id_str")] != coocurrs["{0}_y".format("user.id_str")]]
    coocurrs = coocurrs.drop_duplicates()
    return coocurrs 

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Create user-user networks based on Twitter interactions.")
    parser.add_argument('--source', help="source directory", required = True)
    parser.add_argument('--target', help="target directory", required = True)
    parser.add_argument("--bytag", help="tag to use to find coocurrences. Examples: hashtag, retweeted_user", required = True)
    args = parser.parse_args()
    
    source = args.source
    target = args.target
    if os.path.isdir(source):
        for input_file in os.listdir(source):
            input_path = source + str(input_file)
            edgelist = pd.read_csv(input_path, dtype=object)
            edgelist = edgelist[["user.id_str", "hashtag"]].drop_duplicates()
            edgelist['hashtag'] = edgelist['hashtag'].str.lower()
            coocurrs = get_coocurrences(edgelist, args.bytag)
            output_path = target + "/" + input_file[:-4] + "_cooccurrences.csv"
            coocurrs.to_csv(output_path, index=False)
    elif os.path.isfile(source):
        edgelist = pd.read_csv(source, dtype=object)
        coocurrs = get_coocurrences(edgelist, args.bytag)
        coocurrs.to_csv(target, index=False)
    else:
        print("Error finding source directory or file: {0}".format(source))
