'''
This is a flask server that is used in conjuction with an IOS app that tracks
the betting information of a user over time. The flask server uses a database
games_database.db to store information about NBA games such as which teams are
involved, the scores of each time, and the date of the game. This program
scrapes historical information from a google doc of old NBA scores and uses
this information to keep the database up to date. The purpose of this server is
to allow the IOS app to determine if bets from the user are won are lost. These
outcome of these individual bets are determined based on information provided by
the server.
'''



from flask import Flask
app = Flask(__name__)

import sqlite3
import urllib.request
import io
import pandas
import json



def check_table_exists(database, table):
    '''
    Function that returns a boolean value of whether a particular table exists
    in a particular databse
    '''

    #connect to database
    database_cursor,database_connection = connect_to_database(database)

    #find if this table exists
    database_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='games_and_scores'")

    return len(database_cursor.fetchall()) > 0


def clear_table(database, table):
    '''
    Function that clears a desired table from a particular database. Used to
    reset the table when there is dummy data inside that we no longer want if
    table exists.
    Returns: Nothing
    '''

    #check if table exists
    if check_table_exists('games_database.db','games_and_scores'):
        #connect to database
        database_cursor,database_connection = connect_to_database(database)

        database_cursor.execute("DROP TABLE " + table)


def connect_to_database(database_name):
    '''
    Function that creates the connection between the database and then returns
    a cursor to the database and a connection to the database. The database_name
    is the database which you would like to connect to.
    Returns: a list of first, the cursor for desired database, and second, the
             connection to the database
    '''

    #create connection object to represent database
    conn = sqlite3.connect(database_name)

    #create cursor object to use with database
    db_cursor = conn.cursor()

    return db_cursor, conn


@app.route('/sendGameInfo/<string:team1>/<string:team2>/<string:date>')
def collect_game_info(team1, team2, date):
    '''
    This function is how the server takes in new games that the user has bet on.
    This utilizes the input_game_data function to put this information into the
    database with token scores of -1 to signify we do not have real scores for
    this particular event.
    '''

    input_game_data(date, team1, -1, team2, -1)

    return "Data has been stored in table"



@app.route('/getGameInfo/<string:team1_name>/<string:team2_name>/<string:date_of_game>')
def return_game_info(team1_name, team2_name, date_of_game):
    '''
    Function that searches for given information in database. If the data is
    not present, then we return just a string saying no such game. Otherwise, we
    return the scores from the database along with the given information.
    '''

    #check if table exists
    if check_table_exists('games_database.db','games_and_scores'):

        #connect to database
        database_cursor,database_connection = connect_to_database('games_database.db')


        #return the score from the desired game
        database_cursor.execute("SELECT team1, team1Score, team2, team2Score, date FROM games_and_scores WHERE (team1 = ? or team1 = ?) AND (team2 = ? or team2 = ?) AND date = ?", (team1_name,team2_name,team1_name,team2_name,date_of_game))

        result = database_cursor.fetchall()

        #looking up game that does not exist
        if not check_value_in_table(date_of_game,team1_name,team2_name):
            return "No such game"


        #return in json format
        team1 = str(result[0][0])
        team1Score = str(result[0][1])
        team2 = str(result[0][2])
        team2Score = str(result[0][3])
        date = str(result[0][4])

        json_info = {"Team1": team1, "Team1Score": team1Score, "Team2": team2, "Team2Score":team2Score, "Date":date}

        return json.dumps(json_info)



def check_value_in_table(game_date, team_1, team_2):
    '''
    Function that returns a boolean value based on if the value is in a table
    '''

    #connect to database
    database_cursor,database_connection = connect_to_database('games_database.db')

    database_cursor.execute("SELECT * FROM games_and_scores WHERE (team1 = ? OR team1 = ?) AND (team2 = ? OR team2 = ?) AND date = ?", (team_1,team_2,team_1,team_2,game_date))

    return len(database_cursor.fetchall()) > 0



def input_game_data(game_date, team_1, team_1_score, team_2, team_2_score):
    '''
    Function that takes all information about one game and either adds it to
    the database or updates the database
    '''

    #connect to database
    database_cursor,database_connection = connect_to_database('games_database.db')

    #check if we need to create table
    database_cursor.execute("CREATE TABLE if not exists games_and_scores"
                            "(team1, team1Score, team2, team2Score, date)")


    in_table = check_value_in_table(game_date, team_1, team_2)


    #if it is already in table just update; otherwise add to table
    if in_table == True:
        database_cursor.execute("update games_and_scores set team1Score = ?, team2Score = ? where team1 = ? and team2 = ? and date = ?", (team_1_score,team_2_score,team_1,team_2,game_date))
    else:
        database_cursor.execute("insert into games_and_scores values(?,?,?,?,?)",(team_1, team_1_score, team_2, team_2_score, game_date))


    #persist changes in database
    database_connection.commit()



@app.route('/scrape')
def scrape_game_data():

    """
    Function that scrapes data from google sheet and only inputs data if the
    game is either not in the database, or the scores are stored as -1
    """
    #link
    game_data_csv = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vSOiUF09fWkEaXBNNC1XhYzXC9FhWKb4W1Gs7Wma5bumtrrSCg22wRysRbfwaBygOD2bfR0MQZwXf_C/pub?output=csv'

    #Get data and store in pandas dataframe
    response = urllib.request.urlopen(game_data_csv)
    data_frame = pandas.read_csv(io.TextIOWrapper(response))

    #iterate over every row and put that data into table if needed
    info_list = []
    for index, row in data_frame.iterrows():
        date, visiting_team, visitor_score, home_team, home_score = row['Date'],row['Visitor/Neutral'],row['PTS'],row['Home/Neutral'],row['PTS.1']
        # remove day from game
        date = date[5:]
        #input data into database
        input_game_data(date, visiting_team, visitor_score, home_team, home_score)


    return "Data scraped"


@app.route('/clearDatabase')
def clear_database():
    '''
    Function that clears the table in the databse
    '''
    #check if table exists and if so remove table
    if check_table_exists('games_database.db','games_and_scores'):

        clear_table('games_database.db','games_and_scores')

        return "Table removed from Database"

    else:
        #database already clear
        return "No such table"


@app.route('/checkDatabase')
def show_database():
    '''
    Function that returns all information in the database in one long string.
    '''

    #connect to database
    database_cursor,database_connection = connect_to_database('games_database.db')

    #check if table exists
    if check_table_exists('games_database.db','games_and_scores'):
        #get everything form database
        database_cursor.execute("SELECT * FROM games_and_scores")

        res = database_cursor.fetchall()

        #retrun everything from the table
        return str(res)

    else:
        #database is empty
        return "Nothing in database"



if __name__ == '__main__':
    app.run()
