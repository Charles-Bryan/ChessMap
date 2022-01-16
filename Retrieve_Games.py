"""
Purpose is to get the chess games for the user. Starting with Lichess and may expand to Chess.com in the future
"""
import requests # for downloading games
import cProfile

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
    for line in r.iter_lines(chunk_size=2048):
        if line: # skips over blank lines
            print(line)



if __name__ == '__main__':
    cProfile.run('retrieve_data()')
