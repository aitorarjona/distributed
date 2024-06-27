import time

from distributed import Client
import dask.bag as db

MAP_SIZE = 20


def sleepy(i):
    t0 = time.time()
    print("Going to sleep...")
    time.sleep(3)
    t1 = time.time()
    return (i, t0, t1)


if __name__ == "__main__":

    # cluster = FargateCluster(
    #     image="macarronesc0lithops/daskfargate:02",
    #     fargate_spot=False,
    #     worker_cpu=1024,
    #     worker_mem=2048,
    #     n_workers=0,
    # )
    # cluster.adapt(minimum=0, maximum=MAP_SIZE)
    # tcluster = time.perf_counter()
    # print("Cluster creation time: ", tcluster - t0)

    # create dask client
    client = Client("ws://127.0.0.1:8888", connection_limit=1)

    input("Press Enter to continue...")

    t0 = time.perf_counter()
    b = db.from_sequence([(i,) for i in range(MAP_SIZE)]).map(sleepy)
    res = client.compute(b).result()
    t1 = time.perf_counter()

    print(res)

    print("Exec time: ", t1 - t0)
