from threading import Thread


class Worker(Thread):

    def __init__(self, name, job_queue, confirm_queue):
        Thread.__init__(self)
        self.name = name
        self.job_queue = job_queue
        self.confirm_queue = confirm_queue
        self.start()

    def run(self):
        while True:
            self.work()

    def work(self):
        pass
