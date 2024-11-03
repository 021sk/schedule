import schedulee
import time

def job():
    print("im working")

schedulee.every(5).seconds.do(job)

while True:
    schedulee.run_pending()
    time.sleep(1)
