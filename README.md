**Status: Abandoned. This was not the approach we were looking for and thus, it's going nowhere.**

**Eventually I'll remove this repo in an effort to curb the entropy of the universe.**

moztradamus
===========

A presence hack, current {ver}sion is "0"

SERVER
------

### GET /{ver}/ping/*{optional token}*

Use this to register your token's ping.

#### Returns

the token (or generates one if not present). Please use the token for
subsequent pings, sharing with friends, etc.

e.g.

    "m7b8iprM+zaAIW6nZ1g=="


### POST /{ver}/poll/

Use this to return a hash of pings and their freshness.

#### POST body

Body is a comma delimited list of tokens.

e.g.

    "8NPm7b8iprM+zaAIW6nZ1g==,Ro7hnty8SGqwl4ySkZO6Kg=="

#### Return

Return is a JSON hash of the tokens and freshness values.

e.g.

    {"8NPm7b8iprM+zaAIW6nZ1g==":138,
     "Ro7hnty8SGqwl4ySkZO6Kg==":63}

where the value of the token is the number of seconds since the last
time the token called /ping/

if the user has not checked in within 15 minutes, or no longer exists,
that token is not returned. It's up to the client to determine how to
deal with that. I'm not here to tell you Billy doesn't love you
anymore.

## Notes:

The idea here was not to disclose any personally identifying
information. I could have made the token something like an email
address, but decided not to because PII. The freshness date on an
entry is there because it's the minimal amount of data that a server
can hold and still be useful. It's up to the client to contain the
graph of people associated with the tokens, contact info, etc. The
client sorts them into whatever fashion the end user finds
interesting.
