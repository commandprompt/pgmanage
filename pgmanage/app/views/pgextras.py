from django.http import JsonResponse
from app.utils.decorators import database_required_new, user_authenticated

@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def get_pgcron_jobs(request, v_database):

    response_data = {'data': None, 'status': 'success'}
    try:
        job_rows = v_database.QueryPgCronJobs().Rows
        response_data['data'] = {'jobs': [{"id": int(job[0]), "name":job[1]} for job in job_rows]}
    except Exception as exc:
        response_data['status'] = 'failed'

    return JsonResponse(response_data)

@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def get_pgcron_job_details(request, database):
    data = request.data
    job_meta = data.get('job_meta', None)
    if job_meta:
        job = database.GetPgCronJob(job_meta.get('id'))
        if not job.Rows:
            return JsonResponse({
                "data": f"Job does not exist."
            }, status=400)
        [job_details] = job.Rows

        return JsonResponse(data=dict(job_details))

    return JsonResponse(data={'data': 'invalid job details supplied'}, status=400)

@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def get_pgcron_job_logs(request, database):
    data = request.data
    job_meta = data.get('job_meta', None)
    if job_meta:
        logs = database.GetPgCronJobLogs(job_meta.get('id'))
        [stats] = database.GetPgCronJobStats(job_meta.get('id')).Rows
        return JsonResponse(data={'logs':logs.Rows, 'stats': dict(stats)}, safe=False)

    return JsonResponse(data={'data': 'invalid job details supplied'}, status=400)

@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def save_pgcron_job(request, database):
    data = request.data
    try:
        database.SavePgCronJob(data.get('jobName'), data.get('schedule'), data.get('command'), data.get('inDatabase'))
    except Exception as exc:
        return JsonResponse(data={'data': str(exc)}, status=500)

    return JsonResponse({})

@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def delete_pgcron_job(request, database):
    data = request.data
    job_meta = data.get('job_meta', None)
    if job_meta:
        try:
            database.DeletePgCronJob(job_meta['id'])
            return JsonResponse({"status": "success"})

        except Exception as exc:
            return JsonResponse(data={'data': str(exc)}, status=500)

    return JsonResponse(data={'data': 'invalid job details supplied'}, status=400)

@user_authenticated
@database_required_new(check_timeout=True, open_connection=True)
def delete_pgcron_job_logs(request, database):
    data = request.data
    job_meta = data.get('job_meta', None)
    if job_meta:
        try:
            database.DeletePgCronJobLogs(job_meta['id'])
            return JsonResponse({"status": "success"})

        except Exception as exc:
            return JsonResponse(data={'data': str(exc)}, status=500)

    return JsonResponse(data={'data': 'invalid job details supplied'}, status=400)