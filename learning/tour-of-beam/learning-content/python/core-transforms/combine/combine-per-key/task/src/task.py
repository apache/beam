import apache_beam as beam

from log_elements import LogElements

# Players as keys for combinations
PLAYER_1 = 'Player 1'
PLAYER_2 = 'Player 2'
PLAYER_3 = 'Player 3'

with beam.Pipeline() as p:

    # Setting different values for keys
    (p | beam.Create([(PLAYER_1, 15), (PLAYER_2, 10), (PLAYER_1, 100),
                      (PLAYER_3, 25), (PLAYER_2, 75)])
    # There is a summation of the value for each key using a combination
     | beam.CombinePerKey(sum)
     | LogElements())