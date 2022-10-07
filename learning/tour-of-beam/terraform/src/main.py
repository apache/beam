import os
import logging

def http_handler(request):
    """
    HTTP Cloud Function that returns the value of FOO.
    Args:
        request (flask.Request): The request object.
        <http://flask.pocoo.org/docs/1.0/api/#flask.Request>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>.
    """

    logging.info('Handler Started!')

    response_body = f'This is a message for Cloud Function HTTP Trigger'
    return response_body