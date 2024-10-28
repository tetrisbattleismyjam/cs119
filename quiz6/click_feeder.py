#!/usr/bin/env python3

import importlib
import os
import sys

from click_gen import *
max_evs = 1000000         # Total number of events that will be generated. 
                       # If you want to make it run forever, set this number to like a million

n_senders = 3000        # Number of senders
n_queries = 60         # How many different queries (or messages) a sender can send

dispatcher = Dispatcher()
sender_names = shuffle(['sndr%04d' % i for i in range(n_senders)])
senders = [dispatcher.add_sender(Sender(sender_name, n_queries, n_senders*0.003, n_senders*0.003)) for sender_name in sender_names]
counter = 0

for ev in dispatcher.launch():
    for sender in senders:
      mean, std = sender.delay
      sender.delay = (max(0.3, mean - 0.3), std)
      
    print(ev, flush=True)
    max_evs -= 1
    if max_evs == 0:
        break
