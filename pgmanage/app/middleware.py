import json


class DataUnpackMiddleware:
    """
    Middleware to unpack JSON data in Django views.

    This middleware checks the content type of incoming POST requests. If the content type is 'application/json',
    it unpacks the JSON data from the request body and assigns it to `request.data`. Otherwise, it unpacks the
    JSON data from the 'data' POST parameter and assigns it to `request.data`.
    """

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        if request.method == "POST":
            if request.content_type == "application/json":
                request.data = json.loads(request.body)
            else:
                request.data = json.loads(request.POST.get("data", "{}"))

        response = self.get_response(request)

        return response
