import psycopg2
import select
import subprocess
from dotenv import load_dotenv
import os
load_dotenv()

RENDER_DATABASE_URL = os.getenv("DATABASE_INDIA")
ENVIRONMENT_PATH = os.getenv("ENVIRONMENT_PATH")
PROGRAM_PATH = os.getenv("PROGRAM_PATH")


conn = psycopg2.connect(RENDER_DATABASE_URL)
conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
cur = conn.cursor()
cur.execute("LISTEN task_channel;")
print("Listening for new tasks...")

while True:
    if select.select([conn], [], [], 5) == ([], [], []):
        continue  # timeout, loop again
    conn.poll()
    while conn.notifies:
        notify = conn.notifies.pop(0)
        task_id = notify.payload
        print(f"Triggering run_worker.py for task {task_id}")
        log_path = "/home/frede/listener/worker.log"
        # Spawn the full worker script in its own process
        with open(log_path, "ab") as f:
            subprocess.Popen(
                [ENVIRONMENT_PATH, PROGRAM_PATH],
                stdout=f,
                stderr=f
            )



