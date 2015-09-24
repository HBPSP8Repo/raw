import subprocess
import time

class Docker:
    def __init__(self):
        self.cid = ""

    def start(self, dataset):
        cmd = "docker run -d -p 5001:5000 raw/ldb //raw/scripts/run-with-gcc-backend.sh " + dataset
        print "Starting docker:", cmd
        self.cid = subprocess.check_output(cmd, shell=True).strip()

    def waitForLDBContainer(self):
        i=0
        while i<20:
            logs = subprocess.check_output("docker logs " + self.cid, shell=True, stderr=subprocess.STDOUT)
            if "Running on http://" in logs:
                print "LDB server started. Container logs:\n" + logs
                return
            if "Exceptions.LDBException: internal error" in logs:
                subprocess.check_output("docker logs " + self.cid, shell=True)
                raise RuntimeWarning("Web server failed to start")
            i+=1
            print "Waiting for LDB web server to start:", i
            time.sleep(1)
        subprocess.check_output("docker logs " + self.cid, shell=True)
        raise RuntimeError("Giving up waiting for python server")

    def stop(self):
        cmd = "docker stop -t 0 " + self.cid + " && docker rm " + self.cid
        print cmd
        try:
            status = subprocess.call(cmd, shell=True)
            print "Status:", status
        except:
            pass
