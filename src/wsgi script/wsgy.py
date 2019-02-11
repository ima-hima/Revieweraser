import cgi
import redis
import sys
import urllib

# import urllib.parse
from json import loads, dumps

def application(environ, start_response):
    ''' Accept a list of values to look up in Redis db. Get results from redis for each of those values.
        Return results as JSON. '''

    # Make sure there's something in there.
    # url   = urllib.request.Request.full_url
    # query = urllib.parse.urlparse(environ['QUERY_STRING'])).query
    query_body = urllib.parse.parse_qs(environ['QUERY_STRING']) #environ['QUERY_STRING'])

    # reviews = '{\"reviews\":[18778586,23310293,3]}'

    # check to make sure an actualy query came through
    if not query_body:
        return_values = {'hhh': 'yyy'}
    else:
        r = redis.Redis(host="52.72.55.132", port=6379, db=1)

        # query_body is in form {query: answer, query: answer} loop over keys and put
        user_ids = []
        for user_id in query_body:
            user_ids += query_body[user_id]
        # Now I have list of user ids, loop over that and get resulsts from Redis
        return_values = {}
        for this_id in user_ids:
            # get hash from Redis
            from_redis_dict = r.hgetall(this_id)
            interim_dict = {}
            for key in from_redis_dict:
                # All the keys in the Redis hash and their values must be decoded and put into a dictionary.
                # This dictionary is then stored as the value in a dictionary with the user id as a key.
                interim_dict[key.decode('utf-8')] = (from_redis_dict[key]).decode('utf-8')
            return_values[this_id] = interim_dict


    # # Now, back to JSON
    json_output = dumps(return_values)

    status = '200 OK'
    html = bytes(json_output, encoding='utf-8') # This constant encoding and decoding is driving me crazy.
    response_header = [('Content-type','application/json')]
    start_response(status, response_header)
    return [html]

