import cgi
import redis
import sys
import urllib
from json import loads, dumps

def application(environ, start_response):
    r = redis.Redis(host="ec2-35-174-194-26.compute-1.amazonaws.com", port=6379, db=1)

    # Make sure there's something in there.
    # try:
    #     request_body_size = int(environ.get('CONTENT_LENGTH', 0))
    # except (ValueError):
    #     request_body_size = 0
    reviews = '{\"reviews\":[18778586,23310293,3]}'

    # request_body = environ['wsgi.input'].read(request_body_size)
    json_reviews  = loads(reviews) # At this point reviews ought to be a list of ints.
    return_values = {'reviews':[]}
    for item in json_reviews['reviews']:
        return_values['reviews'] += item
    #     if redis.exists(item):
    #         return_values += r.get(item) # Each item is a hash {'num': int, 'stars': int, 'words': int} )

    # Now, back to JSON
    json_output = dumps(return_values)
    # json_output = str(json_reviews)
    # json_output = "{\"reviews\":[1,2,3]}"

    status = '200 OK'
    html = '<html>\n' \
           '<body>\n' \
           '<div style="width: 100%; font-size: 40px; font-weight: bold; text-align: center;">\n' \
           'Welcome to mod_wsgi Test Page\n' + str(sys.path) + \
           '</div>\n' \
           '</body>\n' \
           '</html>\n'
    response_header = [('Content-type','text/plain')]
    start_response(status, response_header)
    return [json_output]
