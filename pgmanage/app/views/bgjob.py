from app.bgjob.jobs import BatchJob
from app.utils.decorators import user_authenticated
from django.http import HttpResponse, JsonResponse


@user_authenticated
def index(request):
    job_list = BatchJob.list(request.user)
    return JsonResponse(data={"data": job_list})


@user_authenticated
def delete_job(request, job_id):
    try:
        BatchJob.delete(job_id, request.user)
        return HttpResponse(status=204)
    except LookupError as exc:
        return JsonResponse(data={"data": str(exc)}, status=410)


@user_authenticated
def details(request, job_id, out=-1, err=-1):
    try:
        job = BatchJob(id=job_id)

        return JsonResponse(data={"data": job.status(out, err)})
    except LookupError as exc:
        return JsonResponse(data={"data": str(exc)}, status=410)


@user_authenticated
def stop_job(request, job_id):
    try:
        BatchJob.stop_job(job_id, request.user)
        return HttpResponse(status=204)
    except LookupError as exc:
        return JsonResponse(data={"data": str(exc)}, status=410)
