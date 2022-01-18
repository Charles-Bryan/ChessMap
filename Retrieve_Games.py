"""
Purpose is to get the chess games for the user. Starting with Lichess and may expand to Chess.com in the future

Build Process
1. First just make a reliable scrip tthat will save all games to a datatable
2. Then compare if it is faster tot clean the data in the datatable vs when parsing the games
      a) Likely leaving the actual game text until already in a data table regardless
"""
import requests # for downloading games
import cProfile
import numpy as np  # Internet says numpy is better for large data than lists
import datatable as dt  # Trying datatable instead of pandas. Supposed oto be faster.
import pandas as pd

def retrieve_data(site="Lichess", user="E4_is_Better"):
    """
    Starting off with no filters
    timing tests using my data:
    Line by line: 113984 function calls (113903 primitive calls) in 83.475 seconds
        'r = requests.get(url, stream=True)'
    All at once: 56303 function calls (52932 primitive calls) in 83.478 seconds
        'r = requests.get(url)'
    chunk_size = 2048: 90930 function calls (90849 primitive calls) in 83.456 seconds
        'r = requests.get(url, stream=True)
        for line in r.iter_lines(chunk_size=2048)'
    :param site:
    :param user:
    :return:
    """

    # https://lichess.org/api/games/user/E4_is_Better?tags=true&clocks=false&evals=false&opening=false
    url = 'https://lichess.org/api/games/user/' + user + '?tags=true&clocks=false&evals=false&opening=false'
    # r = requests.get(url)
    r = requests.get(url, stream=True)

    # retrieve_game(r)
    all_games = []
    game = []
    for raw_line in r.iter_lines():
        # retrieve_game(r)
        if raw_line:  # skips over blank lines
            line = raw_line.decode('UTF-8')
            game.append(line)  #
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

            if line[0] == '1':
                if len(game) != 17:
                    print(f"Length should be 17, is {len(game)}")
                all_games.append(game)
                game = []

    # Not sure the best way to transpose in datatable, so doing it in numpy first....
    df1 = pd.DataFrame(all_games, columns=['Event', 'Site', 'Date', 'White', 'Black', 'Result', 'UTCDate', 'UTCTime',
                                           'WhiteElo', 'BlackElo', 'WhiteRatingDiff', 'BlackRatingDiff', 'Variant',
                                           'TimeControl', 'ECO', 'Termination', 'Moves'])
    print(0)

if __name__ == '__main__':
    cProfile.run('retrieve_data()')
