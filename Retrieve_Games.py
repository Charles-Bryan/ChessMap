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

import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)
import plotly.express as px


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
    black_list = ['BlackTitle', 'WhiteTitle', 'Event', 'Site', 'UTCDate', 'UTCTime', 'Variant', 'TimeControl',
                  'ECO', 'Termination', 'FEN', 'SetUp']
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
            elif str(line[0]) == '1':
                if line[0:3] != '1. ':
                    ignore_game = True  # Ignores games from preset position because they start like '1...'
                elif '?' in temp_game.values():
                    # Has caught ? being in the Elo columns when playing bots
                    ignore_game = True
                else:
                    temp_game['Raw_Moves'] = line

                if ignore_game:  # Need to catch variants here like Crazyhouse
                    temp_game = default_game.copy()
                    ignore_game = False
                else:
                    all_games.append(temp_game)
                    temp_game = default_game.copy()
            elif str(line[0:3]) == '0-1':
                # Case of abandoned game where the outcome is 0-1
                temp_game = default_game.copy()
                ignore_game = False

            else:
                # Have had the program crash
                print(f"Unexpected line: {line}")

    return pd.DataFrame(all_games)


def process_nonmove_cols(input_df, player_name):
    df = input_df.copy()

    # drop the rating diff columns. Keeping just for later performance analysis project
    df.drop(columns=['WhiteRatingDiff', 'BlackRatingDiff'], inplace=True)

    # Only starting with the following columns:
    #   White, Black, Result, Raw_Moves, Date, WhiteElo, BlackElo

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
        'Outcome': 'float16',
    })
    df['Date'] = df['Date'].dt.date

    return df


def process_moves(input_df, num_moves):
    df = input_df.copy()

    df['remove_result'] = [x.rsplit(" ", 1)[0] for x in df["Raw_Moves"]]
    df['append_game_end'] = df['remove_result'].astype(str) + ' Game_End'

    # This regex captures space-digit(s)-decimal, or start of string-digit(s)-decimal in the case of '1.'
    df["replace"] = df["append_game_end"].str.replace(pat='(^| )\d+\. ', repl=' ', regex=True)

    # This splits the move column into separate ply columns
    moves_df = df["replace"].str.split(pat=' ', n=2 * num_moves + 1, expand=True)

    # First column is just a space. Drop it.
    moves_df.drop(columns=[0, 2 * num_moves + 1], inplace=True)

    # Name the columns based on their ply
    move_cols = ["ply_" + str(x) for x in moves_df.columns]
    moves_df.columns = move_cols

    # join the move columns with the other provided info
    output_df = moves_df.join(input_df.drop(columns=["Raw_Moves"]))

    return output_df


def prep_for_plotly(input_df, path):
    df = input_df.copy()

    # Column of 1s to aggregate into the number of occurrences
    df['Count'] = 1
    # add columns for # of wins, # of losses, # of draws.
    df["Wins"] = 0
    df.loc[df["Outcome"] == 1.0, 'Wins'] = 1
    df["Losses"] = 0
    df.loc[df["Outcome"] == 0.0, 'Losses'] = 1
    df["Draws"] = 0
    df.loc[df["Outcome"] == 0.5, 'Draws'] = 1

    df_agg = df.groupby(path).agg(Avg_Result=('Outcome', 'mean'),
                                  Occurrences=('Count', 'sum'),
                                  Wins=('Wins', 'sum'),
                                  Losses=('Losses', 'sum'),
                                  Draws=('Draws', 'sum'),
                                  Last_Date=('Date', 'max')
                                  ).reset_index()

    return df_agg


def create_customdata(df, fig, path):
    # Create the custom data to add to our figure, this gives actually useful hovertext despite plotly's best attempts

    # Get the ids form the figure
    id_df = pd.DataFrame(data=fig.data[0].ids, columns=['ids'])

    agg_list = []
    for i in range(len(path)):
        agg_path = path[:len(path) - i]
        temp_agg = df.groupby(agg_path).agg(
            Avg_Result=('Avg_Result', 'mean'),
            Occurrences=('Occurrences', 'sum'),
            Wins=('Wins', 'sum'),
            Losses=('Losses', 'sum'),
            Draws=('Draws', 'sum'),
            Last_Date=('Last_Date', 'max')
        ).reset_index()

        # Join our path in the format plotly did to match their ids
        temp_agg['ids'] = temp_agg[agg_path].apply("/".join, axis=1).values

        # Drop the path columns only keeping the aggregated columns.
        temp_agg.drop(columns=agg_path, inplace=True)
        agg_list.append(temp_agg)

    agg_data = pd.concat(agg_list)
    # Add a parent id to see hw frequent a move is out of the parent move
    agg_data['parent_id'] = agg_data['ids'].str.rsplit('/', n=1, expand=True)[0]

    # Join our custom data against the id's to make sure we are passing the right data
    custom_join = id_df.merge(agg_data[['ids', 'Avg_Result', 'Occurrences', 'Wins', 'Losses', 'Draws', 'Last_Date',
                                        'parent_id']], how='left', on='ids')

    custom_join = custom_join.merge(agg_data[['Occurrences', 'ids']],
                              how='left', on=None, left_on='parent_id', right_on='ids', suffixes=('', '_parent'))

    if custom_join.isnull().values.any():
        print(f"{custom_join.isnull().values.sum()} nan's detected when generating customdata. ")

    # Create any columns I need from the data
    # 1. Percentage of parent
    custom_join['percent_of_parent'] = 100*custom_join['Occurrences']/custom_join['Occurrences_parent']
    # 2. Percentage of total(use max value of the Occurrences column)
    total_games = custom_join['Occurrences'].max()
    custom_join['percent_of_total'] = 100*custom_join['Occurrences']/total_games

    # Format Columns how I want their values displayed
    #   Avg_Result - round and then convert to a string for precision. pands' round function is broken
    custom_join['Avg_Result'] = custom_join['Avg_Result'].astype(float).round(2).astype(str).str.slice(0, 5)
    custom_join['percent_of_total'] = custom_join['percent_of_total'].astype(float).round(2).astype(str).str.slice(0, 5)
    custom_join['percent_of_parent'] = custom_join['percent_of_parent'].astype(float).round(2).astype(str).str.slice(0, 5)
    # Not sure if I will need the ids for anything later. If I do I can skip the drop
    return custom_join.drop(columns=['ids', 'parent_id', 'ids_parent'])


def main():
    full_move_cutoff = 3


    # Input data from user
    inputs = {
        "Player": 'thekkid',
        "Player_Color": 'White',    # White, Black, Both
        "Opponent": 'All',          # Username, All
        "UltraBullet": True,        # True, False
        "Bullet": True,             # True, False
        "Blitz": True,              # True, False
        "Rapid": True,              # True, False
        "Classical": True,          # True, False
        "Correspondence": True,     # True, False
        "Mode": 'Both',             # Rated, Casual, Both
        "Start_Date": dt.datetime(2010, 1, 22, 0, 0),  # None, datetime object from plotly
        "End_Date": 'None',         # None, datetime object from plotly
        "Site": 'Lichess'           # Lichess. Maybe expand to Chess.com one day
    }

    # Get the raw game data
    url = create_url(inputs)
    raw_game_df = retrieve_data(url)

    # Process the data for features I am interested in
    partial_processed_df = process_nonmove_cols(input_df=raw_game_df, player_name=inputs["Player"])
    processed_df = process_moves(input_df=partial_processed_df, num_moves=full_move_cutoff)

    processed_df['ply_0'] = 'All Games'
    path = ["ply_" + str(x) for x in range(0, 2 * full_move_cutoff + 1)]
    # Further process the data to work with plotly
    final_df = prep_for_plotly(processed_df, path)

    fig = px.treemap(final_df,
                     path=path,
                     values='Occurrences',
                     color='Avg_Result',
                     color_continuous_scale=["black", "white", "blue"],
                     color_continuous_midpoint=0.5,
                     branchvalues='total')

    # Add in our customdata
    customdata = create_customdata(final_df, fig, path)
    # customdata[0] = Avg_Result
    # customdata[1] = Occurrences
    # customdata[2] = Wins
    # customdata[3] = Losses
    # customdata[4] = Draws
    # customdata[5] = Last_Date
    # customdata[6] = Occurrences_parent
    # customdata[7] = percent_of_parent
    # customdata[8] = percent_of_total
    fig.data[0].customdata = customdata
    # Modify the Hover Template to use our Custom Data
    fig.data[0].hovertemplate = 'Move: %{label}<br>' \
                                'Occurrences: %{value}<br>' \
                                'Average Result: %{customdata[0]}<br>' \
                                'Percentage of Parent: %{customdata[7]}<br>'\
                                'Percentage of Total Games: %{customdata[8]}<br>'\
                                '<br>' \
                                'Wins:   %{customdata[2]}<br>' \
                                'Losses: %{customdata[3]}<br>' \
                                'Draws:  %{customdata[4]}<br>' \
                                '<br>' \
                                'Last Played: %{customdata[5]}'

    fig.show()


if __name__ == '__main__':
    cProfile.run('main()')
