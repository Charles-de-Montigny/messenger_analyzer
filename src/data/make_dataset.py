# -*- coding: utf-8 -*-
#!/usr/bin/env python3

import json
import pandas as pd

def make_dataset(input_path):
    """Load JSON file and transform it into pandas DataFrames.
    
    Args:
        input_path: str
            The path to the JSON file.
    
    Returns:
        Messages pandas DataFrame
        Participants pandas DataFrame
    """
    # Load JSON file
    with open(input_path) as json_data:
        d = json.load(json_data)
    # Messages
    tmp = []
    for message in d['messages']:
        tmp.append(message)
    messages = pd.DataFrame(tmp)
    messages['time'] = pd.to_datetime(messages['timestamp_ms'], unit = 'ms')
    messages['date'] = pd.to_datetime(messages['timestamp_ms'], unit = 'ms').dt.strftime('%Y-%m-%d')
    messages['month'] = pd.to_datetime(messages['timestamp_ms'], unit = 'ms').dt.strftime('%Y-%m')
    messages['id'] = messages.index
    # Participants
    tmp = []
    for participant in d['participants']:
        tmp.append(participant)
    participants = pd.DataFrame(tmp)
    return messages, participants

if __name__ == '__main__':
    messages, participants = make_dataset("data/raw/sundayrose/message_1.json")
    import pdb; pdb.set_trace()