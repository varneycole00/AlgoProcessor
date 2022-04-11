import subprocess

if __name__ == "__main__":  # confirms that the code is under main function
    proc = subprocess.run("python3 consumer.py & python3 producer.py", shell=True, check=True)
