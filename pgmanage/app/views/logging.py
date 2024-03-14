import logging

from django.http import HttpResponse


logger = logging.getLogger(__name__)


def log_message(request):
    for log in request.data.get("logs", []):
        extra = {"request_id": log.pop("request_id")}
        level = log.get("level", "error")
        message = log.get("msg")
        getattr(logger, level)(message, extra=extra)

    return HttpResponse(status=200)
