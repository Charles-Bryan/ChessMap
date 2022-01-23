"""
Purpose is to get the chess games for the user. Starting with Lichess and may expand to Chess.com in the future

Build Process
1. First just make a reliable scrip tthat will save all games to a datatable
2. Then compare if it is faster tot clean the data in the datatable vs when parsing the games
      a) Likely leaving the actual game text until already in a data table regardless
"""
import requests  # for downloading games
import cProfile
import pandas as pd
import numpy as np
import datetime as dt
import time


def create_url(inputs):
    # default parts
    prefix = f"https://lichess.org/api/games/user/{inputs['Player']}?"
    # tags=true is required for any details other than just moves
    mid = f"tags=true&clocks=false&evals=false&opening=false"

    # pre-mid sections. These will append a '&' as a suffix

    # 1. color
    match inputs['Player_Color']:
        case 'Both':
            color_str = ''
        case 'White':
            color_str = 'color=white&'
        case 'Black':
            color_str = 'color=black&'

    # 2. opponent
    if inputs['Opponent'] == 'All':
        opponent_str = ''
    else:
        opponent_str = f"vs={inputs['Opponent']}&"

    # 3. mode (rated or casual)
    match inputs['Mode']:
        case 'Both':
            mode_str = ''
        case 'Rated':
            mode_str = 'rated=true&'
        case 'Casual':
            mode_str = 'rated=false&'

    # post-mid sections. These sections will prefix the '&' and not append

    # 4. start date
    if inputs['Start_Date'] == 'None':
        start_date_str = ''
    else:
        t_ms = int((inputs['Start_Date'] - dt.datetime(1970, 1, 1)).total_seconds()) * 1000
        start_date_str = f'&since={t_ms}'

    # 5. end date
    if inputs['End_Date'] == 'None':
        end_date_str = ''
    else:
        t_ms = int((inputs['End_Date'] - dt.datetime(1970, 1, 1)).total_seconds()) * 1000
        end_date_str = f'&until={t_ms}'

    # 6. gametypes
    available_gametypes = {
        "UltraBullet": 'ultraBullet',
        "Bullet": 'bullet',
        "Blitz": 'blitz',
        "Rapid": 'rapid',
        "Classical": 'classical',
        "Correspondence": 'correspondence'
    }
    selected_gametypes = []
    for key, value in available_gametypes.items():
        if inputs[key]:
            selected_gametypes.append(value)
    gametypes_str = f"&perfType={'%2C'.join(selected_gametypes)}"

    return prefix + color_str + opponent_str + mode_str + mid + start_date_str + end_date_str + gametypes_str


def retrieve_data(url):
    """
    Just finished making the url
    """
    default_game = {'Raw_Moves': None}
    white_list = ['White', 'Black', 'Date', 'Result', 'WhiteElo', 'BlackElo', 'WhiteRatingDiff', 'BlackRatingDiff']
    black_list = ['Event', 'Site', 'UTCDate', 'UTCTime', 'Variant', 'TimeControl', 'ECO', 'Termination', 'FEN', 'SetUp']
    for e in white_list:
        default_game[e] = None

    r = requests.get(url, stream=True)
    if r.status_code == 429:
        print(f"We got a 429!")
        while r.status_code == '429':
            time.sleep(60)
            r = requests.get(url, stream=True)
    elif r.status_code != 200:
        print(f"Unexpected response status code {r.status_code}.")

    all_games = []
    temp_game = default_game.copy()
    ignore_game = False  # Used to detect if a Variant (and potentially other cases are caught)
    for raw_line in r.iter_lines():
        if raw_line:  # skips over blank lines
            line = raw_line.decode('UTF-8')
            if line[0] == '[':
                space_loc = line.find(' ')
                if line[1:space_loc] in black_list:
                    continue
                elif line[1:space_loc] in white_list:  # makes sure the tag is in our white list
                    if temp_game[line[1:space_loc]] is None:
                        temp_game[line[1:space_loc]] = line[space_loc + 2:-2]
                    else:
                        print("debug: shouldnt be overwriting the same value")
                        ignore_game = True
                else:
                    print(f"debug: Unexpected tag {line[1:space_loc]}")
            elif line[0].isdigit():
                if line[0] != '1':
                    ignore_game = True
                else:
                    temp_game['Raw_Moves'] = line

                if ignore_game:  # Need to catch variants here like Crazyhouse
                    temp_game = default_game.copy()
                    ignore_game = False
                else:
                    all_games.append(temp_game)
                    temp_game = default_game.copy()
            else:
                print(f"Unexpected line: {line}")

    return pd.DataFrame(all_games)


def process_df_cols(player_name, input_df):
    df = input_df.copy()

    # drop the rating diff columns. Keeping just for later performance analysis project
    df.drop(columns=['WhiteRatingDiff', 'BlackRatingDiff'], inplace=True)

    # Only starting with the following columns:
    #   White, Black, Result, Raw_Moves, Date, WhiteElo, BlackElo

    # Save player color for later?
    # df['Player_Color'] = np.where(df['White'] == player_name, 'White', 'Black')

    # opponent_elo
    df['OpponentElo'] = np.where(df['White'] == player_name, df['BlackElo'], df['WhiteElo'])

    # parsing result
    result_conditions = [
        (df['White'] == player_name) & (df['Result'] == '1-0'),  # Win as white
        (df['Black'] == player_name) & (df['Result'] == '0-1'),  # Win as black
        (df['White'] == player_name) & (df['Result'] == '0-1'),  # Loss as white
        (df['Black'] == player_name) & (df['Result'] == '1-0'),  # Loss as black
        (df['Result'] == '1/2-1/2')  # draw
    ]
    result_values = [1, 1, 0, 0, 0.5]  # win, win, loss, loss, draw
    df['Outcome'] = np.select(result_conditions, result_values)

    df.drop(columns=['White', 'Black', 'Result', 'WhiteElo', 'BlackElo'], inplace=True)

    # force column types to save a little memory
    df = df.astype({
        'Date': 'datetime64',
        'OpponentElo': 'int16',
        'Outcome': 'float16'
    })

    return df


def main():
    inputs = {
        "Player": 'E4_is_Better',
        "Player_Color": 'Both',  # White, Black, Both
        "Opponent": 'All',  # Username, All
        "UltraBullet": True,  # True, False
        "Bullet": False,  # True, False
        "Blitz": True,  # True, False
        "Rapid": True,  # True, False
        "Classical": False,  # True, False
        "Correspondence": False,  # True, False
        "Mode": 'Both',  # Rated, Casual, Both
        "Start_Date": dt.datetime(2010, 1, 22, 0, 0),  # None, datetime object from plotly
        "End_Date": 'None',  # None, datetime object from plotly
        "Site": 'Lichess'  # Lichess. Maybe expand to Chess.com one day
    }
    url = create_url(inputs)
    raw_game_df = retrieve_data(url)

    cleaned_game_df = process_df_cols(player_name=inputs["Player"], input_df=raw_game_df)


if __name__ == '__main__':
    cProfile.run('main()')



