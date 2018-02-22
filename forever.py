#!/usr/bin/env python
# -*- coding: utf-8 -*-

import subprocess
import time

popen = subprocess.Popen(['python','run_spider.py'],shell=False)
n = 1
while True:
    time.sleep(300)
    if popen.poll()==0:
        popen = subprocess.Popen(['python', 'run_spider.py'], shell=False)
        n += 1
        time.sleep(1)
    if n==5:
        break