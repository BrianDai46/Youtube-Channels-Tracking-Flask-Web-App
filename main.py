from flask.views import MethodView
from flask import Flask, render_template, request, redirect, url_for, make_response
from etl import youtubeFetcher
import psycopg2, sys, csv
from io import StringIO

# Import required libraries and initialize the Flask app
app = Flask(__name__)

# Function to read the API key from a file
def read_api_key(file_path):
    with open(file_path, 'r') as file:
        return file.read().strip()

# Read the API key from the file
api_key_file_path = 'youtube_dev_api_key.txt'
dev_api_key = read_api_key(api_key_file_path)
# Initialize the YouTube Fetcher class with the API key
fetcher = youtubeFetcher(dev_api_key)

# Function to establish a connection to the PostgreSQL database
def connect(param_dic):
    conn = None
    try:
        conn = psycopg2.connect(**param_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    return conn

# Define database connection parameters
param_dic = {
    "host": "localhost",
    "port": "5432",
    "database": "youtube_load",
    "user": "postgres",
    "password": "123"
}
# Connect to the PostgreSQL database
conn = connect(param_dic)

# Function to create the required tables in the PostgreSQL database
def create_database(connection):
    cursor = connection.cursor()
    # Create the channel_statistics table
    create1 ="CREATE TABLE IF NOT EXISTS channel_statistics ( \
    Channel_id VARCHAR(255) PRIMARY KEY, \
    Channel_name VARCHAR(255), \
    Subscribers INT, \
    Views BIGINT, \
    Total_videos INT, \
    playlist_id VARCHAR(255), \
    creation_date TIMESTAMPTZ, \
    age_days INT, \
    age_years INT \
);"
    cursor.execute(create1)
    conn.commit()
    # Create the video_statistics table
    create2 ="CREATE TABLE IF NOT EXISTS video_statistics ( \
        video_id VARCHAR(255) PRIMARY KEY, \
        video_title VARCHAR(255), \
        Channel_id VARCHAR(255), \
        Channel_name VARCHAR(255), \
        category_id VARCHAR(255), \
        category_name VARCHAR(255), \
        description TEXT, \
        tags VARCHAR(255), \
        view_count INT, \
        like_count INT, \
        comment_count INT, \
        Published_date TIMESTAMPTZ, \
        age_days INT, \
        age_years INT \
    );"
    cursor.execute(create2)
    conn.commit()
    # Create the video_comments table
    create3 ="CREATE TABLE IF NOT EXISTS video_comments ( \
        comment_id INT, \
        video_id VARCHAR(255), \
        author VARCHAR(255), \
        text TEXT, \
        likes INT, \
        FOREIGN KEY (video_id) REFERENCES video_statistics (video_id), \
        PRIMARY KEY (comment_id, video_id) \
    );"
    cursor.execute(create3)
    # Add a unique index to the video_comments table
    modify="CREATE UNIQUE INDEX IF NOT EXISTS index ON video_comments (comment_id, video_id);"
    cursor.execute(modify)
    conn.commit()
# Function to clear all the data from the tables in the PostgreSQL database
def clear_database(connection):
    cursor = connection.cursor()
    tables = ['channel_statistics', 'video_statistics', 'video_comments']
    # Truncate each table
    for table in tables:
        query = f"TRUNCATE {table} CASCADE;"
        cursor.execute(query)
    connection.commit()
    cursor.close()

# Function to get data from the specified table in the PostgreSQL database
def get_data(connection, table_name, limitation=10, sort_by=None):
    cursor = connection.cursor()
    query = f"SELECT * FROM {table_name} "
    # Add sorting condition if provided
    if sort_by:
        query += f" ORDER BY {sort_by} DESC"
    # Add a limit to the number of rows returned
    if limitation:
        query += f" LIMIT {limitation}"
    query += ";"
    cursor.execute(query)
    data = cursor.fetchall()
    return data

# HomePage class to handle the main page of the application
class HomePage(MethodView):
    strings = []
    # GET method to display the main page
    def get(self):
        return render_template("index.html", strings=self.strings, channel_id=None, channel_name=None)
    # POST method to process the form submission on the main page
    def post(self):
        input_string = request.form['input_string']
        self.strings.append(input_string)
        return render_template('index.html', strings=self.strings, channel_id=None, channel_name=None)
    # Function to insert data into the database
    @staticmethod
    def insert_data():
        channel_stats = fetcher.get_channel_stats(HomePage.strings)
        video_stats = fetcher.get_channel_video_stats(HomePage.strings)
        comment_stats = fetcher.get_comment_info(HomePage.strings)
        fetcher.update_data(conn, channel_stats, 'channel_statistics', 'Channel_id')
        fetcher.update_data(conn, video_stats, 'video_statistics', 'video_id')
        fetcher.update_data(conn, comment_stats, 'video_comments', 'comment_id, video_id')
        HomePage.strings = []
        return HomePage.strings
    # Function to clear the strings list
    def clear_strings(self):
        self.strings = []
        return redirect(url_for('home_page'))

# ChannelPage class to handle the channel rankings page
class ChannelPage(MethodView):
    # GET method to display the channel rankings page
    def get(self):
        sort_by = request.args.get('sort_by', None)
        download = request.args.get('download', None)
        headings = [
            "Channel_id", "Channel_name", "Subscribers", "Views", "Total_videos",
            "playlist_id", "creation_date", "age_days", "age_years"
        ]
        data = get_data(conn, 'channel_statistics', sort_by=sort_by)
        if download:
            return self.download_csv(headings, get_data(conn, 'channel_statistics', limitation=None, sort_by=sort_by))
        return render_template("channel_ranking.html", headings=headings, data=data, sort_by=sort_by)
    # Function to download the channel rankings as a CSV file
    def download_csv(self, headings, data):
        si = StringIO()
        cw = csv.writer(si)
        cw.writerow(headings)
        cw.writerows(data)
        output = make_response(si.getvalue())
        output.headers["Content-Disposition"] = "attachment; filename=channel_statistics.csv"
        output.headers["Content-type"] = "text/csv"
        return output

# VideoPage class to handle the video statistics page
class VideoPage(MethodView):
    # GET method to display the video statistics page
    def get(self):
        sort_by = request.args.get('sort_by', None)
        download = request.args.get('download', None)
        headings = [
            "video_id", "video_title", "Channel_id", "Channel_name", "category_id",
            "category_name", "description", "tags", "view_count", "like_count",
            "comment_count", "Published_date", "age_days", "age_years"
        ]
        data = get_data(conn, 'video_statistics', sort_by=sort_by)
        if download:
            return self.download_csv(headings, get_data(conn, 'video_statistics', limitation=None, sort_by=sort_by))
        return render_template("video_statistic.html", headings=headings, data=data, sort_by=sort_by)
    # Function to download the video statistics as a CSV file
    def download_csv(self, headings, data):
        si = StringIO()
        cw = csv.writer(si)
        cw.writerow(headings)
        cw.writerows(data)
        output = make_response(si.getvalue())
        output.headers["Content-Disposition"] = "attachment; filename=videos_statistic.csv"
        output.headers["Content-type"] = "text/csv"
        return output

# CommentPage class to handle the comment statistics page
class CommentPage(MethodView):
    # GET method to display the comment statistics page
    def get(self):
        sort_by = request.args.get('sort_by', None)
        download = request.args.get('download', None)
        headings = [
            "comment_id", "video_id", "author", "text", "likes"
        ]
        data = get_data(conn, 'video_comments', sort_by=sort_by)
        if download:
            return self.download_csv(headings, get_data(conn, 'video_comments', limitation=None, sort_by=sort_by))
        return render_template("comment_statistic.html", headings=headings, data=data, sort_by=sort_by)
    # Function to download the comment statistics as a CSV file
    def download_csv(self, headings, data):
        si = StringIO()
        cw = csv.writer(si)
        cw.writerow(headings)
        cw.writerows(data)
        output = make_response(si.getvalue())
        output.headers["Content-Disposition"] = "attachment; filename=comments_statistic.csv"
        output.headers["Content-type"] = "text/csv"
        return output

# Route to insert data into the database
@app.route('/insert_data')
def insert_data_route():
    HomePage.strings = HomePage.insert_data()
    return render_template('index.html', strings=HomePage.strings, channel_id=None, channel_name=None)

# Route to clear the strings list
@app.route('/clear', methods=['POST'])
def clear_strings_route():
    HomePage.clear_strings(HomePage)
    return HomePage.clear_strings(HomePage)

# Route to clear the database
@app.route('/clear_database', methods=['POST'])
def clear_database_route():
    clear_database(conn)
    return redirect(url_for('home_page'))

# Route to lookup a channel ID by its name
@app.route('/channel_id_lookup', methods=['POST'])
def channel_id_lookup():
    channel_name = request.form['channel_name']
    channel_id = fetcher.fetch_channel_id(channel_name)
    if channel_id != None:
        fetcher.get_channel_thumbnail(channel_id, 'static\channel_thumbnail.png')
    return render_template('index.html', channel_id=channel_id, channel_name = channel_name, strings=HomePage.strings)

# Add URL rules for HomePage, ChannelPage, VideoPage, and CommentPage
app.add_url_rule('/', view_func=HomePage.as_view('home_page'), methods=['GET', 'POST'])
app.add_url_rule('/channels', view_func=ChannelPage.as_view('channels_page'), methods=['GET'])
app.add_url_rule('/videos', view_func=VideoPage.as_view('videos_page'), methods=['GET'])
app.add_url_rule('/comments', view_func=CommentPage.as_view('comments_page'), methods=['GET'])
# Create the database tables if they do not exist
create_database(conn)
# Start the Flask app in debug mode
app.run(debug=True)
