from dask.distributed import as_completed

def runner(client):
    jobs = []
    for i in range(5):
        job = client.submit(combine)
        jobs.append(job)

    for future in as_completed(jobs):
        result = future.result()
        print(result)

def combine():
    return ['test','list']