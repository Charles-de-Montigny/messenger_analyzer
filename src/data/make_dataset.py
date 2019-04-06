# -*- coding: utf-8 -*-
#!/usr/bin/env python3

import json
import pandas as pd

def extract_contents(df, content_col):
    """Extract not null contents from the columns, rename the content columns
    'content' and creates a content_type column.
    
    Args:
        df: pandas DataFrame
            The DataFrame from which the content is extracted.
        content_col: str
            The column from which to extract the content from.
    
    Returns: A pandas DataFrame with the id column and the content column.
    """
    content_df = df.loc[pd.notnull(df[content_col]), [content_col, 'id']]
    content_df.rename(columns={content_col:'content'}, inplace = True)
    content_df['content_type'] = content_col
    return content_df

def make_messages(dict):
    """Returns clean message DataFrame from the dictionnary.
    
    Args:
        dict: python dictionnary

    Returns: A pandas DataFrame with content, id, content_type, sender_name and time.
    """
    # Unpackaed messages from dict
    tmp = []
    for message in dict['messages']:
        tmp.append(message)
    messages = pd.DataFrame(tmp)
    messages['time'] = pd.to_datetime(messages['timestamp_ms'], unit = 'ms')
    messages['id'] = messages.index
    messages = messages.query('type == "Generic"')

    # Reshape DataFrame into a single content columns
    content = messages.loc[pd.notnull(messages['content']), ['content', 'id']]
    content['content_type'] = 'messages'

    contents = [content]
    content_cols = ['audio_files', 'files', 'gifs', 'photos', 'share', 'sticker', 'videos']
    for col in content_cols:
        contents.append(extract_contents(messages, col))
    contents_df = pd.concat(contents).sort_values('id')
    messages_df = pd.merge(contents_df, messages[['sender_name', 'time', 'id']], on = 'id')
    return messages_df

def make_participants(dict):
    """Returns a DataFrame with the participants.
    
    Args:
        dict: python dictionnary

    Returns: A pandas DataFrame
    """
    tmp = []
    for participant in dict['participants']:
        tmp.append(participant)
    return pd.DataFrame(tmp)

def make_text(messages_df):
    """Returns a string that contains all the messages.
    
    Args:
        messages_df: Pandas DataFrame

    Returns: A pandas DataFrame
    """
    pass





def make_reactions(dict):
    """Returns a DataFrame with the reactions.
    
    Args:
        dict: python dictionnary

    Returns: A pandas DataFrame
    """
    # Unpackaed messages from dict
    tmp = []
    for message in dict['messages']:
        tmp.append(message)
    messages = pd.DataFrame(tmp)
    reactions_dict = messages.loc[:, ['reactions']]
    reactions_dict.dropna(inplace = True)
    reactions_df = pd.DataFrame()
    for idx, row in reactions_dict.iterrows():
        tmp = pd.DataFrame(row['reactions'])
        tmp['id'] = idx
        reactions_df = reactions_df.append(tmp)
    return reactions_df

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
    messages_df = make_messages(d)
    # Participants
    participants_df = make_participants(d)
    # Reactions
    reactions_df = make_reactions(d)
    return messages_df, participants_df, reactions_df


if __name__ == '__main__':
    messages, participants, reactions = make_dataset("data/raw/sundayrose/message_1.json")
    import pdb; pdb.set_trace()