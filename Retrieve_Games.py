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
import datetime as dt


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
        # start_dt = fun(inputs['Start_Date'])
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
    default_game = {
        'Event': None,
        'Site': None,
        'Date': None,
        'White': None,
        'Black': None,
        'Result': None,
        'UTCDate': None,
        'UTCTime': None,
        'WhiteElo': None,
        'BlackElo': None,
        'WhiteRatingDiff': None,
        'BlackRatingDiff': None,
        'Variant': None,
        'TimeControl': None,
        'ECO': None,
        'Termination': None,
        'Raw_Moves': None
    }

    r = requests.get(url, stream=True)

    all_games = []
    temp_game = default_game.copy()
    ignore_game = False  # Used to detect if a Variant (and potentially other cases are caught)
    for raw_line in r.iter_lines():

        if raw_line:  # skips over blank lines
            line = raw_line.decode('UTF-8')
            if line[0] == '[':
                space_loc = line.find(' ')
                # temp_game[line[1:space_loc]] = line[space_loc+2:-2]  # Use this after testing
                if line[
                   1:space_loc] not in default_game.keys():  # Easier/Safer to just compare vs a white list then to make this black list
                    ignore_game = True
                elif temp_game[line[1:space_loc]] == None:
                    temp_game[line[1:space_loc]] = line[space_loc + 2:-2]
                else:
                    print(
                        f"Error! Updating {line[1:space_loc]} from {temp_game[line[1:space_loc]]} to {line[space_loc + 2:-2]}. It should be None if we are going to update it.")
            elif line[0].isdigit():
                temp_game['Raw_Moves'] = line
                if ignore_game:  # Need to catch variants here like Crazyhouse
                    ignore_game = False
                else:
                    all_games.append(temp_game)
                temp_game = default_game.copy()
            else:
                print(f"Unexpected line: {line}")

            # b'[Event "Rated Bullet game"]'
            # b'[Site "https://lichess.org/CvirGZ84"]'
            # b'[Date "2022.01.16"]'
            # b'[White "E4_is_Better"]'
            # b'[Black "iamali"]'
            # b'[Result "0-1"]'
            # b'[UTCDate "2022.01.16"]'
            # b'[UTCTime "15:08:15"]'
            # b'[WhiteElo "1580"]'
            # b'[BlackElo "1594"]'
            # b'[WhiteRatingDiff "-9"]'
            # b'[BlackRatingDiff "+5"]'
            # b'[Variant "Standard"]'
            # b'[TimeControl "120+1"]'
            # b'[ECO "B21"]'
            # b'[Termination "Time forfeit"]'
            # b'1. e4

    # Not sure the best way to transpose in datatable, so doing it in numpy first....
    return pd.DataFrame(all_games)


def process_df_cols(input_df):
    df = input_df.copy()
    # Event - Likely Drop and use the Rating Dif to determine if rated or casual
    # Site - Drop for now
    # Date - Not needed
    # White - Needed to determine player color, opponent name, and opponent color
    # Black - Needed to determine player color, opponent name, and opponent color
    # Result - Used to determine Win/Loss/Draw
    # UTCDate - Likely Drop
    # UTCTime - Likely Drop
    # WhiteElo - Keep for performance stuff later
    # BlackElo - Keep for performance stuff later
    # WhiteRatingDiff - Likely Drop. If Rated or Casual is wanted, they need to select it
    # BlackRatingDiff - Likely Drop. If Rated or Casual is wanted, they need to select it
    # Variant - Likely Drop. Games will be selected through API
    # TimeControl - Likely Drop
    # ECO - Likely Drop
    # Termination - Will need to see what unique options there are and if that will affect dropping rows.
    #   Use a player like german11 to see all possibilities.

    # TimeControl
    #   Splitting into StartClock(in seconds) and increment(in seconds)
    df['TimeControl'] = df['TimeControl'].str.replace('-', '99999+0')
    df[['StartClock', 'Increment']] = df['TimeControl'].str.split('+', n=1, expand=True).apply(pd.to_numeric)

    # testing

    print(0)
    test = df["StartClock"] + 40 * df["Increment"]
    print(0)

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

    cleaned_game_df = process_df_cols(raw_game_df)


if __name__ == '__main__':
    cProfile.run('main()')



