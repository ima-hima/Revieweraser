import cgi
import redis
import sys
import urllib

# import urllib.parse
from json import loads, dumps

def application(environ, start_response):
    ''' Accept a list of values to look up in Redis db. Get results from redis for each of those values.
        Return results as JSON. '''
    r = redis.Redis(host="52.72.55.132", port=6379, db=1)
    input_ids = [29024061]
    return_values = {}
    for this_id in input_ids:
        r.hmset(this_id, {'num': '1', 'stars': '2', 'words': '3'} )
        a = r.hgetall(this_id)
        this_dict = {}
        for key in a:
            this_dict[key.decode('utf-8')] = (a[key]).decode('utf-8')
        return_values[this_id] = this_dict

    # Make sure there's something in there.
    # url   = urllib.request.Request.full_url
    # query = urllib.parse.urlparse(url).query

    reviews = '{\"reviews\":[18778586,23310293,3]}'

    # request_body = urllib.parse.parse_qs('reviews=18778586') #environ['QUERY_STRING'])
    # json_reviews  = loads(reviews) # At this point reviews ought to be a list of ints.
    # return_values = {'reviews': []}
    # for item in json_reviews['reviews']:
    #     return_values['reviews'] += 5
    # #     if redis.exists(item):
    # #         return_values += r.get(item) # Each item is a hash {'num': int, 'stars': int, 'words': int} )

    # # Now, back to JSON
    json_output = dumps(return_values)
    # json_output = str(json_reviews)
    # json_output = "{\"reviews\":[1,2,3]}"

    status = '200 OK'
    just_text = '{"500": "600", "this": { "this": "is text"}}'
    html = bytes(json_output, encoding='utf-8')
    response_header = [('Content-type','application/json')]
    start_response(status, response_header)
    return [html]

    # status = '200 OK'
    # output = bytes('<html>\n<body>\nHello World!\n' + str(sys.path) + '\n</body>\n</html>', encoding='utf-8')

    # response_headers = [('Content-type', 'text/html'),
    #                     ('Content-Length', str(len(output)))]
    # start_response(status, response_headers)

    # return [output]
