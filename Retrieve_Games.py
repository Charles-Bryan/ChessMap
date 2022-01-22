"""
Purpose is to get the chess games for the user. Starting with Lichess and may expand to Chess.com in the future

Build Process
1. First just make a reliable scrip tthat will save all games to a datatable
2. Then compare if it is faster tot clean the data in the datatable vs when parsing the games
      a) Likely leaving the actual game text until already in a data table regardless
"""
import requests # for downloading games
import cProfile
import pandas as pd

def create_url(inputs):
    # default
    prefix = f"https://lichess.org/api/games/user/{inputs['Player']}?"
    mid = f"tags=false&clocks=false&evals=false&opening=false"

    # pre-mid sections. These will append a '&' as a suffix
    # 1. color
    match inputs['Player_Color']:
        case 'Both':
            print('Both')
        case 'White':
            print('White')
        case 'Black':
            print('Black')
    # post-mid sections. These sections will prefix the '&' and not append
    # start/end dates


def retrieve_data(site="Lichess", user="E4_is_Better"):
    """
    Starting off with no filters
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
    # https://lichess.org/api/games/user/E4_is_Better?tags=true&clocks=false&evals=false&opening=false
    url = 'https://lichess.org/api/games/user/' + user + '?tags=true&clocks=false&evals=false&opening=false'
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
                if line[1:space_loc] not in default_game.keys():  # Easier/Safer to just compare vs a white list then to make this black list
                    ignore_game = True
                elif temp_game[line[1:space_loc]] == None:
                     temp_game[line[1:space_loc]] = line[space_loc + 2:-2]
                else:
                    print(f"Error! Updating {line[1:space_loc]} from {temp_game[line[1:space_loc]]} to {line[space_loc+2:-2]}. It should be None if we are going to update it.")
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
    # Termination - Will need to see what unique options there are and if that will affect dropping rows. Use a player like german11 to see all possibilities.

    # TimeControl
    #   Splitting into StartClock(in seconds) and increment(in seconds)
    df['TimeControl'] = df['TimeControl'].str.replace('-', '99999+0')
    df[['StartClock', 'Increment']] = df['TimeControl'].str.split('+', n=1, expand=True).apply(pd.to_numeric)

    #testing
    print(0)
    test = df["StartClock"] + 40*df["Increment"]
    print(0)

def main():
    inputs = {
        "Player": 'E4_is_Better',
        "Player_Color": 'Both',     # White, Black, Both
        "Opponent": 'All',          # Username, All
        "UltraBullet": 'Yes',       # Yes, No
        "Bullet": 'Yes',            # Yes, No
        "Blitz": 'Yes',             # Yes, No
        "Rapid": 'Yes',             # Yes, No
        "Classical": 'Yes',         # Yes, No
        "Correspondence": 'Yes',    # Yes, No
        "Mode": 'Both',             # Rated, Casual, Both
        "Start_Date": '1990-01-30', # Dates in a format undecided
        "End_Date": 'Today'         # Today or date in format above
    }
    url = create_url(inputs)
    df = retrieve_data()
    df = process_df_cols(df)


if __name__ == '__main__':
    cProfile.run('main()')
