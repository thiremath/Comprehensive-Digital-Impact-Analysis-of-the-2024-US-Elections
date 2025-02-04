# # # from flask import Flask, render_template, jsonify, request
# # # import pandas as pd
# # # from dash import Dash, dcc, html, Input, Output

# # # # Initialize Flask app
# # # app = Flask(__name__)

# # # # Read and preprocess Chan data
# # # chan_data_path = "/home/djagdale/SMDP_PRJ3/local_dashboard/data/channew.csv"
# # # chan_data = pd.read_csv(chan_data_path)

# # # # Convert timestamps
# # # chan_data['time'] = pd.to_datetime(chan_data['time'], unit='s')
# # # chan_data['hour'] = chan_data['time'].dt.floor('h')
# # # chan_hourly_counts = chan_data.groupby('hour').size().reset_index(name='chan_count')

# # # # Ensure 'hour' columns are datetime64[ns]
# # # chan_hourly_counts['hour'] = pd.to_datetime(chan_hourly_counts['hour'])

# # # # Initialize Dash app
# # # dash_app = Dash(__name__, server=app, url_base_pathname='/dashboard/')

# # # # Dash layout
# # # dash_app.layout = html.Div([
# # #     html.H1("4chan Dashboard Analysis", style={'text-align': 'center'}),

# # #     # Dropdown menu to select visualization
# # #     dcc.Dropdown(
# # #         id='graph-type',
# # #         options=[
# # #             {'label': '4chan Comments Per Hour', 'value': 'comments'},
# # #             {'label': 'Sentiment Analysis Over Time', 'value': 'sentiment'}
# # #         ],
# # #         value='comments',
# # #         style={'width': '50%', 'margin': '10px auto'}
# # #     ),

# # #     # Input fields for date filtering
# # #     dcc.Input(id='start-date', type='text', placeholder='YYYY-MM-DD', debounce=True),
# # #     dcc.Input(id='end-date', type='text', placeholder='YYYY-MM-DD', debounce=True),
# # #     html.Button('Filter', id='filter-button', n_clicks=0),

# # #     # Graph visualization area
# # #     dcc.Graph(id='time-series-chart')
# # # ])


# # # @dash_app.callback(
# # #     Output('time-series-chart', 'figure'),
# # #     [
# # #         Input('graph-type', 'value'),
# # #         Input('start-date', 'value'),
# # #         Input('end-date', 'value'),
# # #         Input('filter-button', 'n_clicks')
# # #     ]
# # # )
# # # def update_chart(graph_type, start_date, end_date, n_clicks):
# # #     if graph_type == 'comments':
# # #         # Handle Reddit comment visualization
# # #         if start_date and end_date:
# # #             start_date = pd.to_datetime(start_date)
# # #             end_date = pd.to_datetime(end_date)
# # #             filtered_data = chan_hourly_counts[
# # #                 (chan_hourly_counts['hour'] >= start_date) & (chan_hourly_counts['hour'] <= end_date)
# # #             ]
# # #         else:
# # #             filtered_data = chan_hourly_counts

# # #         figure = {
# # #             'data': [
# # #                 {'x': filtered_data['hour'], 'y': filtered_data['chan_count'], 'type': 'line', 'name': '4chan Comments'}
# # #             ],
# # #             'layout': {'title': '4chan Comments Per Hour'}
# # #         }
# # #         return figure

# # #     elif graph_type == 'sentiment':
# # #         # Placeholder for Sentiment Analysis Graph
# # #         figure = {
# # #             'data': [],
# # #             'layout': {'title': 'Sentiment Analysis Placeholder'}
# # #         }
# # #         return figure


# # # @app.route('/')
# # # def index():
# # #     return render_template('index.html')


# # # @app.route('/api/data')
# # # def get_data():
# # #     return jsonify(chan_hourly_counts.to_dict(orient='records'))


# # # if __name__ == '__main__':
# # #     app.run(debug=True)





# # # from flask import Flask, render_template, jsonify, request
# # # import pandas as pd
# # # import re
# # # from dash import Dash, dcc, html, Input, Output


# # # # Initialize Flask app
# # # app = Flask(__name__)


# # # # Function: Preprocess 4chan data for hourly counts visualization
# # # def preprocess_4chan_data():
# # #     chan_data_path = "/home/djagdale/SMDP_PRJ3/local_dashboard/data/channew.csv"
# # #     chan_data = pd.read_csv(chan_data_path)

# # #     # Convert timestamps
# # #     chan_data['time'] = pd.to_datetime(chan_data['time'], unit='s')
# # #     chan_data['hour'] = chan_data['time'].dt.floor('h')
# # #     chan_hourly_counts = chan_data.groupby('hour').size().reset_index(name='chan_count')
# # #     chan_hourly_counts['hour'] = pd.to_datetime(chan_hourly_counts['hour'])

# # #     print("Processed 4chan hourly counts data.")
# # #     print(chan_hourly_counts.head())
# # #     return chan_hourly_counts


# # # # Function to safely extract 'com' data
# # # def extract_com_data(df):
# # #     # Extract the "com" field data safely
# # #     df['com'] = df['485123073'].str.replace('"', '', regex=True).fillna('')
# # #     return df


# # # # Function: Extract and preprocess sentiment data
# # # def preprocess_sentiment_data():
# # #     chan_data_path = "/home/djagdale/SMDP_PRJ3/local_dashboard/data/channew.csv"
# # #     chan_data = pd.read_csv(chan_data_path)

# # #     # Extract the comments safely
# # #     if '485123073' in chan_data.columns:
# # #         chan_data = extract_com_data(chan_data)
# # #     else:
# # #         print("Missing expected 'com' column in the data.")
# # #         return pd.DataFrame()

# # #     # Perform sentiment extraction
# # #     df_filtered['sentiment'] = chan_data['com'].apply(get_sentiment)

# # #     # Handle date and filter only valid data
# # #     df_filtered['time'] = pd.to_datetime(df_filtered['time'], errors='coerce')
# # #     # df_filtered = df_filtered[df_filtered['time'].notna()]

# # #     # Drop rows without sentiment
# # #     df_filtered = chan_data[chan_data['sentiment'].notna()].copy()

# # #     print("*********")
# # #     print(df_filtered['time'])
# # #     # Extract just the dates (group sentiment by date)
# # #     # df_filtered['date'] = df_filtered['time'].dt.date
# # #     sentiment_summary = df_filtered.groupby(['time', 'sentiment']).size().unstack().fillna(0)

# # #     print("Processed sentiment summary data ready for plotting.")
# # #     print(sentiment_summary.head())

# # #     return sentiment_summary


# # # # Sentiment extraction function
# # # def get_sentiment(comment):
# # #     happy_keywords = [
# # #         'happy', 'joy', 'fun', 'smile', 'excited', 'laugh', 'cheerful', 'delighted', 'thrilled', 'content',
# # #         'ecstatic', 'grateful', 'glee', 'blissful', 'elated', 'overjoyed'
# # #     ]
# # #     sad_keywords = [
# # #         'sad', 'cry', 'depressed', 'tears', 'unhappy', 'melancholy', 'mournful', 'sorrowful', 'heartbroken',
# # #         'gloomy', 'despair', 'dismal', 'downhearted'
# # #     ]
# # #     angry_keywords = [
# # #         'angry', 'furious', 'mad', 'rage', 'hate', 'annoyed', 'irritated', 'frustrated', 'outraged',
# # #         'agitated', 'resentful', 'enraged', 'wrathful'
# # #     ]
# # #     hope_keywords = [
# # #         'hope', 'optimism', 'wish', 'believe', 'dream', 'aspire', 'faith', 'positive', 'ambition', 'inspiration',
# # #         'goal', 'trust', 'confidence'
# # #     ]

# # #     comment = comment.lower()

# # #     if any(keyword in comment for keyword in happy_keywords):
# # #         return 'happy'
# # #     elif any(keyword in comment for keyword in sad_keywords):
# # #         return 'sad'
# # #     elif any(keyword in comment for keyword in angry_keywords):
# # #         return 'angry'
# # #     elif any(keyword in comment for keyword in hope_keywords):
# # #         return 'hope'
# # #     else:
# # #         return None


# # # # Initialize Dash app
# # # dash_app = Dash(__name__, server=app, url_base_pathname='/dashboard/')

# # # # Preprocess data
# # # chan_hourly_counts = preprocess_4chan_data()
# # # sentiment_summary = preprocess_sentiment_data()


# # # # Dash Layout
# # # dash_app.layout = html.Div([
# # #     html.H1("4chan Dashboard Analysis", style={'text-align': 'center'}),

# # #     # Graph selection dropdown
# # #     dcc.Dropdown(
# # #         id='graph-type',
# # #         options=[
# # #             {'label': '4chan Comments Per Hour', 'value': 'comments'},
# # #             {'label': 'Sentiment Analysis', 'value': 'sentiment'}
# # #         ],
# # #         value='sentiment',
# # #         style={'width': '50%', 'margin': '10px auto'}
# # #     ),

# # #     # Date range picker for filtering
# # #     dcc.DatePickerRange(
# # #         id='date-picker-range',
# # #         start_date=chan_hourly_counts['hour'].min().date(),
# # #         end_date=chan_hourly_counts['hour'].max().date(),
# # #         display_format='YYYY-MM-DD'
# # #     ),

# # #     # Graph visualization area
# # #     dcc.Graph(id='time-series-chart')
# # # ])


# # # # Define callback to handle graph type selection and date filtering
# # # @dash_app.callback(
# # #     Output('time-series-chart', 'figure'),
# # #     [Input('graph-type', 'value'),
# # #      Input('date-picker-range', 'start_date'),
# # #      Input('date-picker-range', 'end_date')]
# # # )
# # # def update_chart(graph_type, start_date, end_date):
# # #     print(f"Graph type selected: {graph_type}")
# # #     print(f"Filtering data between {start_date} and {end_date}")

# # #     if graph_type == 'comments':
# # #         # Filter data by date range
# # #         filtered_data = chan_hourly_counts[
# # #             (chan_hourly_counts['hour'] >= pd.to_datetime(start_date)) &
# # #             (chan_hourly_counts['hour'] <= pd.to_datetime(end_date))
# # #         ]
# # #         # Create comments graph
# # #         print("Generating filtered comments graph...")
# # #         figure = {
# # #             'data': [
# # #                 {'x': filtered_data['hour'], 'y': filtered_data['chan_count'], 'type': 'line', 'name': 'Comments'}
# # #             ],
# # #             'layout': {'title': 'Filtered 4chan Comments Per Hour'}
# # #         }
# # #         print("Comments graph ready.")
# # #         return figure

# # #     elif graph_type == 'sentiment':
# # #         # Use sentiment_summary data directly (no date range filtering here)
# # #         print("Generating hourly sentiment analysis graph...")
# # #         figure = {
# # #             'data': [
# # #                 {'x': sentiment_summary.index, 'y': sentiment_summary['happy'], 'type': 'bar', 'name': 'Happy'},
# # #                 {'x': sentiment_summary.index, 'y': sentiment_summary['sad'], 'type': 'bar', 'name': 'Sad'},
# # #                 {'x': sentiment_summary.index, 'y': sentiment_summary['angry'], 'type': 'bar', 'name': 'Angry'},
# # #                 {'x': sentiment_summary.index, 'y': sentiment_summary['hope'], 'type': 'bar', 'name': 'Hope'}
# # #             ],
# # #             'layout': {'title': 'Sentiment Analysis by Date'}
# # #         }
# # #         print("Sentiment graph ready.")
# # #         return figure

# # #     # Fallback
# # #     print("No valid graph type selected.")
# # #     return {
# # #         'data': [],
# # #         'layout': {'title': 'No Data'}
# # #     }


# # # @app.route('/')
# # # def index():
# # #     return render_template('index.html')


# # # @app.route('/api/data')
# # # def get_data():
# # #     return jsonify(chan_hourly_counts.to_dict(orient='records'))


# # # if __name__ == '__main__':
# # #     app.run(debug=True)


# # from flask import Flask, render_template, jsonify, request
# # import pandas as pd
# # import re
# # from dash import Dash, dcc, html, Input, Output
# # import plotly.graph_objs as go

# # # Initialize Flask app
# # app = Flask(__name__)

# # # Function: Preprocess 4chan data for hourly counts visualization
# # def preprocess_4chan_data():
# #     chan_data_path = "/home/djagdale/SMDP_PRJ3/local_dashboard/data/channew.csv"
# #     chan_data = pd.read_csv(chan_data_path)

# #     # Convert timestamps
# #     chan_data['time'] = pd.to_datetime(chan_data['time'], unit='s')
# #     chan_data['hour'] = chan_data['time'].dt.floor('h')
# #     chan_hourly_counts = chan_data.groupby('hour').size().reset_index(name='chan_count')
# #     chan_hourly_counts['hour'] = pd.to_datetime(chan_hourly_counts['hour'])

# #     print("Processed 4chan hourly counts data.")
# #     print(chan_hourly_counts.head())
# #     return chan_hourly_counts

# # # Function: Preprocess Reddit data for hourly counts visualization
# # def preprocess_reddit_data():
# #     reddit_data_path = "/home/djagdale/SMDP_PRJ3/local_dashboard/data/filtered_data.csv"
# #     reddit_data = pd.read_csv(reddit_data_path)

# #     if '5' not in reddit_data.columns:
# #         raise ValueError("Reddit dataset must have a column '5' with Unix timestamps.")

# #     # Convert timestamps
# #     reddit_data['time'] = pd.to_datetime(reddit_data['5'])
# #     reddit_data['hour'] = reddit_data['time'].dt.floor('h')
# #     reddit_hourly_counts = reddit_data.groupby('hour').size().reset_index(name='reddit_count')
# #     reddit_hourly_counts['hour'] = pd.to_datetime(reddit_hourly_counts['hour'])

# #     print("Processed Reddit hourly counts data.")
# #     print(reddit_hourly_counts.head())
# #     return reddit_hourly_counts

# # # Function to safely extract 'com' data
# # def extract_com_data(df):
# #     print("&&&&&&&&&&&&")
# #     print(df)
# #     df['com'] = df['485123073']
# #     print("&&&&&&&&&&&&")
# #     print(df['com'])
# #     return df

# # # Sentiment extraction function
# # def get_sentiment(comment):
# #     happy_keywords = [
# #         'happy', 'joy', 'fun', 'smile', 'excited', 'laugh', 'cheerful', 'delighted', 'thrilled', 'content',
# #         'ecstatic', 'grateful', 'glee', 'blissful', 'elated', 'overjoyed'
# #     ]
# #     sad_keywords = [
# #         'sad', 'cry', 'depressed', 'tears', 'unhappy', 'melancholy', 'mournful', 'sorrowful', 'heartbroken',
# #         'gloomy', 'despair', 'dismal', 'downhearted'
# #     ]
# #     angry_keywords = [
# #         'angry', 'furious', 'mad', 'rage', 'hate', 'annoyed', 'irritated', 'frustrated', 'outraged',
# #         'agitated', 'resentful', 'enraged', 'wrathful'
# #     ]
# #     hope_keywords = [
# #         'hope', 'optimism', 'wish', 'believe', 'dream', 'aspire', 'faith', 'positive', 'ambition', 'inspiration',
# #         'goal', 'trust', 'confidence'
# #     ]

# #     comment = str(comment).lower()

# #     if any(keyword in comment for keyword in happy_keywords):
# #         return 'happy'
# #     elif any(keyword in comment for keyword in sad_keywords):
# #         return 'sad'
# #     elif any(keyword in comment for keyword in angry_keywords):
# #         return 'angry'
# #     elif any(keyword in comment for keyword in hope_keywords):
# #         return 'hope'
# #     else:
# #         return None

# # # Function: Extract and preprocess sentiment data
# # def preprocess_sentiment_data():
# #     chan_data_path = "/home/djagdale/SMDP_PRJ3/local_dashboard/data/channew.csv"
# #     chan_data = pd.read_csv(chan_data_path)

# #     # Extract the comments safely
# #     if '485123073' in chan_data.columns:
# #         chan_data = extract_com_data(chan_data)
# #     else:
# #         print("Missing expected 'com' column in the data.")
# #         return pd.DataFrame()

# #     # Perform sentiment extraction
# #     chan_data['sentiment'] = chan_data['com'].apply(get_sentiment)

# #     # Handle date and filter only valid data
# #     chan_data['date'] = pd.to_datetime(chan_data['date'], errors='coerce')

# #     # Drop rows without sentiment
# #     chan_dataa = chan_data[chan_data['sentiment'].notna()].copy()

# #     # Group sentiment by date
# #     sentiment_summary = chan_dataa.groupby([chan_data['date'].dt.date, 'sentiment']).size().unstack().fillna(0)

# #     print("Processed sentiment summary data ready for plotting.")
# #     print(sentiment_summary.head())

# #     return sentiment_summary

# # # Preprocess the data
# # chan_hourly_counts = preprocess_4chan_data()
# # reddit_hourly_counts = preprocess_reddit_data()
# # combined_data = pd.merge(chan_hourly_counts, reddit_hourly_counts, on='hour', how='outer').fillna(0)
# # sentiment_summary = preprocess_sentiment_data()

# # # Initialize Dash app
# # dash_app = Dash(__name__, server=app, url_base_pathname='/dashboard/')

# # # Dash Layout
# # dash_app.layout = html.Div([
# #     html.H1("4chan & Reddit Dashboard Analysis", style={'text-align': 'center'}),

# #     # Data source selection filter
# #     dcc.Checklist(
# #         id='data-source',
# #         options=[
# #             {'label': '4chan', 'value': '4chan'},
# #             {'label': 'Reddit', 'value': 'reddit'}
# #         ],
# #         value=['4chan', 'reddit'],  # Default: Show both
# #         inline=True
# #     ),

# #     # Graph selection dropdown
# #     dcc.Dropdown(
# #         id='graph-type',
# #         options=[
# #             {'label': 'Reddit & 4chan Activity', 'value': 'activity'},
# #             {'label': 'Sentiment Analysis', 'value': 'sentiment'}
# #         ],
# #         value='activity',
# #         style={'width': '50%', 'margin': '10px auto'}
# #     ),

# #     # Date range picker for filtering
# #     dcc.DatePickerRange(
# #         id='date-picker-range',
# #         start_date=combined_data['hour'].min().date(),
# #         end_date=combined_data['hour'].max().date(),
# #         display_format='YYYY-MM-DD'
# #     ),

# #     # Graph visualization area
# #     dcc.Graph(id='time-series-chart')
# # ])

# # # Callback for updating graphs
# # @dash_app.callback(
# #     Output('time-series-chart', 'figure'),
# #     [Input('graph-type', 'value'),
# #      Input('data-source', 'value'),
# #      Input('date-picker-range', 'start_date'),
# #      Input('date-picker-range', 'end_date')]
# # )
# # def update_chart(graph_type, data_source, start_date, end_date):
# #     if graph_type == 'activity':
# #         filtered_data = combined_data[
# #             (combined_data['hour'] >= pd.to_datetime(start_date)) &
# #             (combined_data['hour'] <= pd.to_datetime(end_date))
# #         ]
# #         figure = {
# #             'data': [
# #                 {'x': filtered_data['hour'], 'y': filtered_data['chan_count'], 'type': 'line', 'name': '4chan'},
# #                 {'x': filtered_data['hour'], 'y': filtered_data['reddit_count'], 'type': 'line', 'name': 'Reddit'}
# #             ],
# #             'layout': {'title': 'Combined Reddit & 4chan Activity'}
# #         }
# #     elif graph_type == 'sentiment':
# #         # Convert index to datetime and filter by date range
# #         sentiment_summary_filtered = sentiment_summary.reset_index()
# #         sentiment_summary_filtered['date'] = pd.to_datetime(sentiment_summary_filtered['date'])
# #         sentiment_summary_filtered = sentiment_summary_filtered[
# #             (sentiment_summary_filtered['date'] >= pd.to_datetime(start_date)) &
# #             (sentiment_summary_filtered['date'] <= pd.to_datetime(end_date))
# #         ]
        
# #         figure = {
# #             'data': [
# #                 {'x': sentiment_summary_filtered['date'], 'y': sentiment_summary_filtered['happy'], 'type': 'line', 'name': 'Happy'},
# #                 {'x': sentiment_summary_filtered['date'], 'y': sentiment_summary_filtered['sad'], 'type': 'line', 'name': 'Sad'},
# #                 {'x': sentiment_summary_filtered['date'], 'y': sentiment_summary_filtered['angry'], 'type': 'line', 'name': 'Angry'},
# #                 {'x': sentiment_summary_filtered['date'], 'y': sentiment_summary_filtered['hope'], 'type': 'line', 'name': 'Hope'}
# #             ],
# #             'layout': {
# #                 'title': 'Sentiment Analysis by Date',
# #                 'xaxis': {'title': 'Date'},
# #                 'yaxis': {'title': 'Sentiment Count'}
# #             }
# #         }

# #     return figure

# # @app.route('/')
# # def index():
# #     return render_template('index.html')

# # @app.route('/api/data')
# # def get_data():
# #     return jsonify(combined_data.to_dict(orient='records'))

# # if __name__ == '__main__':
# #     app.run(debug=True)




# from flask import Flask, render_template, jsonify, request
# import pandas as pd
# import re
# from dash import Dash, dcc, html, Input, Output
# import plotly.graph_objs as go

# # Initialize Flask app
# app = Flask(__name__)

# def preprocess_map_data():
#     chan_data_path = "/home/djagdale/SMDP_PRJ3/local_dashboard/data/channew.csv"
#     chan_data = pd.read_csv(chan_data_path)

#     chan_data.columns = chan_data.columns.str.replace(r"[\"']", "", regex=True)
#     print("Columns in chan_data:", chan_data.columns)

#     # Extract country_name and comments
#     if ' md5: idAzKNW97vmSVk6HHpNC8g==' in chan_data.columns and '485123073' in chan_data.columns:
#         chan_data['country_name'] = chan_data[' md5: idAzKNW97vmSVk6HHpNC8g=='].astype(str)
#         chan_data['com'] = chan_data['485123073'].astype(str)
#         chan_data['comment_length'] = chan_data['com'].apply(len)
#         country_comment_counts = chan_data[' md5: idAzKNW97vmSVk6HHpNC8g=='].value_counts()

#         top_15_countries = country_comment_counts.iloc[2:22]



#         top_15_countries_map = top_15_countries.reset_index()
#         top_15_countries_map.columns = ['Country', 'CommentCount']
#     else:
#         print("Required columns missing in the dataset.")
#         return pd.DataFrame({'country_name': [], 'post_count': [], 'avg_comment_length': []})

#     # Group data by country
#     map_data = chan_data.groupby('country_name').agg(
#         post_count=('country_name', 'size'),
#         avg_comment_length=('comment_length', 'mean')
#     ).reset_index()

#     print("Processed map data:")
#     print(map_data.head())
#     return top_15_countries_map


# # Preprocess data for world map
# map_data = preprocess_map_data()
# # if not map_data.empty:
# #     min_comment_length = int(map_data['avg_comment_length'].min())
# #     max_comment_length = int(map_data['avg_comment_length'].max())
# # else:
# #     min_comment_length = 0
# #     max_comment_length = 0

# # Function: Preprocess 4chan data for hourly counts visualization
# def preprocess_4chan_data():
#     chan_data_path = "/home/djagdale/SMDP_PRJ3/local_dashboard/data/channew.csv"
#     chan_data = pd.read_csv(chan_data_path)

#     # Convert timestamps
#     chan_data['time'] = pd.to_datetime(chan_data['time'], unit='s')
#     chan_data['hour'] = chan_data['time'].dt.floor('h')
#     chan_hourly_counts = chan_data.groupby('hour').size().reset_index(name='chan_count')
#     chan_hourly_counts['hour'] = pd.to_datetime(chan_hourly_counts['hour'])

#     print("Processed 4chan hourly counts data.")
#     print(chan_hourly_counts.head())
#     return chan_hourly_counts

# # Function: Preprocess Reddit data for hourly counts visualization
# def preprocess_reddit_data():
#     reddit_data_path = "/home/djagdale/SMDP_PRJ3/local_dashboard/data/filtered_data.csv"
#     reddit_data = pd.read_csv(reddit_data_path)

#     if '5' not in reddit_data.columns:
#         raise ValueError("Reddit dataset must have a column '5' with Unix timestamps.")

#     # Convert timestamps
#     reddit_data['time'] = pd.to_datetime(reddit_data['5'])
#     reddit_data['hour'] = reddit_data['time'].dt.floor('h')
#     reddit_hourly_counts = reddit_data.groupby('hour').size().reset_index(name='reddit_count')
#     reddit_hourly_counts['hour'] = pd.to_datetime(reddit_hourly_counts['hour'])

#     print("Processed Reddit hourly counts data.")
#     print(reddit_hourly_counts.head())
#     return reddit_hourly_counts

# # Function to safely extract 'com' data
# def extract_com_data(df):
#     print("&&&&&&&&&&&&")
#     print(df)
#     df['com'] = df['485123073']
#     print("&&&&&&&&&&&&")
#     print(df['com'])
#     return df

# # Sentiment extraction function
# def get_sentiment(comment):
#     happy_keywords = [
#         'happy', 'joy', 'fun', 'smile', 'excited', 'laugh', 'cheerful', 'delighted', 'thrilled', 'content',
#         'ecstatic', 'grateful', 'glee', 'blissful', 'elated', 'overjoyed'
#     ]
#     sad_keywords = [
#         'sad', 'cry', 'depressed', 'tears', 'unhappy', 'melancholy', 'mournful', 'sorrowful', 'heartbroken',
#         'gloomy', 'despair', 'dismal', 'downhearted'
#     ]
#     angry_keywords = [
#         'angry', 'furious', 'mad', 'rage', 'hate', 'annoyed', 'irritated', 'frustrated', 'outraged',
#         'agitated', 'resentful', 'enraged', 'wrathful'
#     ]
#     hope_keywords = [
#         'hope', 'optimism', 'wish', 'believe', 'dream', 'aspire', 'faith', 'positive', 'ambition', 'inspiration',
#         'goal', 'trust', 'confidence'
#     ]

#     comment = str(comment).lower()

#     if any(keyword in comment for keyword in happy_keywords):
#         return 'happy'
#     elif any(keyword in comment for keyword in sad_keywords):
#         return 'sad'
#     elif any(keyword in comment for keyword in angry_keywords):
#         return 'angry'
#     elif any(keyword in comment for keyword in hope_keywords):
#         return 'hope'
#     else:
#         return None

# # Function: Extract and preprocess sentiment data
# def preprocess_sentiment_data():
#     chan_data_path = "/home/djagdale/SMDP_PRJ3/local_dashboard/data/channew.csv"
#     chan_data = pd.read_csv(chan_data_path)

#     # Extract the comments safely
#     if '485123073' in chan_data.columns:
#         chan_data = extract_com_data(chan_data)
#     else:
#         print("Missing expected 'com' column in the data.")
#         return pd.DataFrame()

#     # Perform sentiment extraction
#     chan_data['sentiment'] = chan_data['com'].apply(get_sentiment)

#     # Handle date and filter only valid data
#     chan_data['date'] = pd.to_datetime(chan_data['date'], errors='coerce')

#     # Drop rows without sentiment
#     chan_dataa = chan_data[chan_data['sentiment'].notna()].copy()

#     # Group sentiment by date
#     sentiment_summary = chan_dataa.groupby([chan_data['date'].dt.date, 'sentiment']).size().unstack().fillna(0)

#     print("Processed sentiment summary data ready for plotting.")
#     print(sentiment_summary.head())

#     return sentiment_summary

# # Preprocess the data
# chan_hourly_counts = preprocess_4chan_data()
# reddit_hourly_counts = preprocess_reddit_data()
# combined_data = pd.merge(chan_hourly_counts, reddit_hourly_counts, on='hour', how='outer').fillna(0)
# sentiment_summary = preprocess_sentiment_data()

# # Initialize Dash app
# dash_app = Dash(__name__, server=app, url_base_pathname='/dashboard/')

# # Dash Layout
# dash_app.layout = html.Div([
#     html.H1("4chan & Reddit Dashboard Analysis", style={'text-align': 'center'}),

#     # Data source selection filter
#     dcc.Checklist(
#         id='data-source',
#         options=[
#             {'label': '4chan', 'value': '4chan'},
#             {'label': 'Reddit', 'value': 'reddit'}
#         ],
#         value=['4chan', 'reddit'],  # Default: Show both
#         inline=True
#     ),

#     # Graph selection dropdown
#     dcc.Dropdown(
#         id='graph-type',
#         options=[
#             {'label': 'Reddit & 4chan Activity', 'value': 'activity'},
#             {'label': 'Sentiment Analysis', 'value': 'sentiment'},
#             {'label': 'World Map', 'value': 'map'}
#         ],
#         value='activity',
#         style={'width': '50%', 'margin': '10px auto'}
#     ),

#     # Date range picker for filtering
#     dcc.DatePickerRange(
#         id='date-picker-range',
#         start_date=combined_data['hour'].min().date(),
#         end_date=combined_data['hour'].max().date(),
#         display_format='YYYY-MM-DD'
#     ),

#     # Add a slider for comment length filter
#     dcc.RangeSlider(
#         id='comment-length-filter',
#         min=0,
#         max=1,
#         step=1,
#         marks={i: str(i) for i in range(0, 1 + 1, 5)},
#         value=[0, 1]
#     ),




#     # Graph visualization area
#     dcc.Graph(id='time-series-chart')
# ])

# @dash_app.callback(
#     Output('time-series-chart', 'figure'),
#     [Input('graph-type', 'value'),
#      Input('data-source', 'value'),
#      Input('date-picker-range', 'start_date'),
#      Input('date-picker-range', 'end_date'),
#      Input('comment-length-filter', 'value')]  # Added Input
# )
# def update_chart(graph_type, data_source, start_date, end_date, comment_length_filter):
#     if graph_type == 'activity':
#         filtered_data = combined_data[
#             (combined_data['hour'] >= pd.to_datetime(start_date)) &
#             (combined_data['hour'] <= pd.to_datetime(end_date))
#         ]
#         figure = {
#             'data': [
#                 {'x': filtered_data['hour'], 'y': filtered_data['chan_count'], 'type': 'line', 'name': '4chan'},
#                 {'x': filtered_data['hour'], 'y': filtered_data['reddit_count'], 'type': 'line', 'name': 'Reddit'}
#             ],
#             'layout': {'title': 'Combined Reddit & 4chan Activity'}
#         }
#     elif graph_type == 'sentiment':
#         # Convert index to datetime and filter by date range
#         sentiment_summary_filtered = sentiment_summary.reset_index()
#         sentiment_summary_filtered['date'] = pd.to_datetime(sentiment_summary_filtered['date'])
#         sentiment_summary_filtered = sentiment_summary_filtered[
#             (sentiment_summary_filtered['date'] >= pd.to_datetime(start_date)) &
#             (sentiment_summary_filtered['date'] <= pd.to_datetime(end_date))
#         ]
        
#         figure = {
#             'data': [
#                 {'x': sentiment_summary_filtered['date'], 'y': sentiment_summary_filtered['happy'], 'type': 'line', 'name': 'Happy'},
#                 {'x': sentiment_summary_filtered['date'], 'y': sentiment_summary_filtered['sad'], 'type': 'line', 'name': 'Sad'},
#                 {'x': sentiment_summary_filtered['date'], 'y': sentiment_summary_filtered['angry'], 'type': 'line', 'name': 'Angry'},
#                 {'x': sentiment_summary_filtered['date'], 'y': sentiment_summary_filtered['hope'], 'type': 'line', 'name': 'Hope'}
#             ],
#             'layout': {
#                 'title': 'Sentiment Analysis by Date',
#                 'xaxis': {'title': 'Date'},
#                 'yaxis': {'title': 'Sentiment Count'}
#             }
#         }

#     elif graph_type == 'map':
#         figure = {
#             'data': [
#                 go.Choropleth(
#                     locations=map_data['Country'],
#                     locationmode='country names',
#                     z=map_data['CommentCount'],
#                     text=map_data['Country'],
#                     colorscale='Viridis',
#                     colorbar_title="Comment Count"
#                 )
#             ],
#             'layout': {
#                 'title': 'World Map of 4chan Posts (Top 15 Countries)',
#                 'geo': {'showframe': False, 'projection': {'type': 'equirectangular'}}
#             }
#         }


#     return figure

# @app.route('/')
# def index():
#     return render_template('index.html')

# @app.route('/api/data')
# def get_data():
#     return jsonify(combined_data.to_dict(orient='records'))

# if __name__ == '__main__':
#     app.run(debug=True)




from flask import Flask, render_template, jsonify, request
import pandas as pd
import re
from dash import Dash, dcc, html, Input, Output
import plotly.graph_objs as go

# Initialize Flask app
app = Flask(__name__)

def preprocess_map_data():
    chan_data_path = "/home/djagdale/SMDP_PRJ3/local_dashboard/data/channew.csv"
    chan_data = pd.read_csv(chan_data_path)

    chan_data.columns = chan_data.columns.str.replace(r"[\"']", "", regex=True)
    print("Columns in chan_data:", chan_data.columns)

    # Extract country_name and comments
    if ' md5: idAzKNW97vmSVk6HHpNC8g==' in chan_data.columns and '485123073' in chan_data.columns:
        chan_data['country_name'] = chan_data[' md5: idAzKNW97vmSVk6HHpNC8g=='].astype(str)
        chan_data['com'] = chan_data['485123073'].astype(str)
        chan_data['comment_length'] = chan_data['com'].apply(len)

        country_comment_counts = chan_data['country_name'].value_counts()
        top_countries_map = country_comment_counts.reset_index()
        top_countries_map.columns = ['Country', 'CommentCount']
    else:
        print("Required columns missing in the dataset.")
        return pd.DataFrame({'Country': [], 'CommentCount': []})

    print("Processed map data:")
    print(top_countries_map.head())
    return top_countries_map

# Preprocess data for world map
map_data = preprocess_map_data()

def preprocess_4chan_data():
    chan_data_path = "/home/djagdale/SMDP_PRJ3/local_dashboard/data/channew.csv"
    chan_data = pd.read_csv(chan_data_path)

    # Convert timestamps
    chan_data['time'] = pd.to_datetime(chan_data['time'], unit='s')
    chan_data['hour'] = chan_data['time'].dt.floor('h')
    chan_hourly_counts = chan_data.groupby('hour').size().reset_index(name='chan_count')
    chan_hourly_counts['hour'] = pd.to_datetime(chan_hourly_counts['hour'])

    print("Processed 4chan hourly counts data.")
    print(chan_hourly_counts.head())
    return chan_hourly_counts

# Function: Preprocess Reddit data for hourly counts visualization
def preprocess_reddit_data():
    reddit_data_path = "/home/djagdale/SMDP_PRJ3/local_dashboard/data/filtered_data.csv"
    reddit_data = pd.read_csv(reddit_data_path)

    if '5' not in reddit_data.columns:
        raise ValueError("Reddit dataset must have a column '5' with Unix timestamps.")

    # Convert timestamps
    reddit_data['time'] = pd.to_datetime(reddit_data['5'])
    reddit_data['hour'] = reddit_data['time'].dt.floor('h')
    reddit_hourly_counts = reddit_data.groupby('hour').size().reset_index(name='reddit_count')
    reddit_hourly_counts['hour'] = pd.to_datetime(reddit_hourly_counts['hour'])

    print("Processed Reddit hourly counts data.")
    print(reddit_hourly_counts.head())
    return reddit_hourly_counts


def extract_com_data(df):
    df['com'] = df['485123073']
    return df

def get_sentiment(comment):
    happy_keywords = ['happy', 'joy', 'fun', 'smile', 'excited', 'laugh', 'cheerful', 'delighted', 'thrilled', 'content']
    sad_keywords = ['sad', 'cry', 'depressed', 'tears', 'unhappy', 'melancholy']
    angry_keywords = ['angry', 'furious', 'mad', 'rage', 'hate', 'annoyed']
    hope_keywords = ['hope', 'optimism', 'wish', 'believe', 'dream']

    comment = str(comment).lower()

    if any(keyword in comment for keyword in happy_keywords):
        return 'happy'
    elif any(keyword in comment for keyword in sad_keywords):
        return 'sad'
    elif any(keyword in comment for keyword in angry_keywords):
        return 'angry'
    elif any(keyword in comment for keyword in hope_keywords):
        return 'hope'
    else:
        return None

def preprocess_sentiment_data():
    chan_data_path = "/home/djagdale/SMDP_PRJ3/local_dashboard/data/channew.csv"
    chan_data = pd.read_csv(chan_data_path)

    if '485123073' in chan_data.columns:
        chan_data = extract_com_data(chan_data)
    else:
        print("Missing expected 'com' column in the data.")
        return pd.DataFrame()

    chan_data['sentiment'] = chan_data['com'].apply(get_sentiment)
    chan_data['date'] = pd.to_datetime(chan_data['date'], errors='coerce')

    chan_dataa = chan_data[chan_data['sentiment'].notna()].copy()
    sentiment_summary = chan_dataa.groupby([chan_data['date'].dt.date, 'sentiment']).size().unstack().fillna(0)

    print("Processed sentiment summary data ready for plotting.")
    print(sentiment_summary.head())

    return sentiment_summary

chan_hourly_counts = preprocess_4chan_data()
reddit_hourly_counts = preprocess_reddit_data()
combined_data = pd.merge(chan_hourly_counts, reddit_hourly_counts, on='hour', how='outer').fillna(0)
sentiment_summary = preprocess_sentiment_data()

# Initialize Dash app
dash_app = Dash(__name__, server=app, url_base_pathname='/dashboard/')

dash_app.layout = html.Div([
    html.H1("4chan & Reddit Dashboard Analysis", style={'text-align': 'center'}),

    dcc.Checklist(
        id='data-source',
        options=[
            {'label': '4chan', 'value': '4chan'},
            {'label': 'Reddit', 'value': 'reddit'}
        ],
        value=['4chan', 'reddit'],
        inline=True
    ),

    dcc.Dropdown(
        id='graph-type',
        options=[
            {'label': 'Reddit & 4chan Activity', 'value': 'activity'},
            {'label': 'Sentiment Analysis', 'value': 'sentiment'},
            {'label': 'World Map', 'value': 'map'}
        ],
        value='activity',
        style={'width': '50%', 'margin': '10px auto'}
    ),

    dcc.DatePickerRange(
        id='date-picker-range',
        start_date=combined_data['hour'].min().date(),
        end_date=combined_data['hour'].max().date(),
        display_format='YYYY-MM-DD'
    ),

    html.Div([
        html.Label("Adjust Slider Range:"),
        html.Label("Select Start Index:"),
        dcc.Slider(
            id='start-idx-slider',
            min=0,
            max=20,
            step=1,
            value=0,  # Default starting index
            marks={i: str(i) for i in range(0, 21)}
        ),
        html.Label("Select End Index:"),
        dcc.Slider(
            id='end-idx-slider',
            min=1,
            max=30,
            step=1,
            value=7,  # Default ending index
            marks={i: str(i) for i in range(1, 31)}
        )
    ], style={'margin': '20px 0'}),



    dcc.Graph(id='time-series-chart')
])

@dash_app.callback(
    Output('time-series-chart', 'figure'),
    [Input('graph-type', 'value'),
     Input('data-source', 'value'),
     Input('date-picker-range', 'start_date'),
     Input('date-picker-range', 'end_date'),
     Input('start-idx-slider', 'value'),  # Pass start index dynamically
     Input('end-idx-slider', 'value')]
)
def update_chart(graph_type, data_source, start_date, end_date, start_idx, end_idx):
    # If data-source selections exist, filter appropriately
    filtered_data = combined_data[
        (combined_data['hour'] >= pd.to_datetime(start_date)) &
        (combined_data['hour'] <= pd.to_datetime(end_date))
    ]

    # Additional filtering based on data source
    if data_source == ['4chan']:
        filtered_data = filtered_data[filtered_data['chan_count'] > 0]
    elif data_source == ['reddit']:
        filtered_data = filtered_data[filtered_data['reddit_count'] > 0]

    if graph_type == 'activity':
        figure = {
            'data': [],
            'layout': {'title': 'Combined Reddit & 4chan Activity'}
        }
        
        if '4chan' in data_source:
            figure['data'].append({
                'x': filtered_data['hour'], 
                'y': filtered_data['chan_count'], 
                'type': 'line', 
                'name': '4chan'
            })
        
        if 'reddit' in data_source:
            figure['data'].append({
                'x': filtered_data['hour'], 
                'y': filtered_data['reddit_count'], 
                'type': 'line', 
                'name': 'Reddit'
            })
    elif graph_type == 'sentiment':
        sentiment_summary_filtered = sentiment_summary.reset_index()
        sentiment_summary_filtered['date'] = pd.to_datetime(sentiment_summary_filtered['date'])
        sentiment_summary_filtered = sentiment_summary_filtered[
            (sentiment_summary_filtered['date'] >= pd.to_datetime(start_date)) &
            (sentiment_summary_filtered['date'] <= pd.to_datetime(end_date))
        ]
        
        figure = {
            'data': [
                {'x': sentiment_summary_filtered['date'], 'y': sentiment_summary_filtered['happy'], 'type': 'line', 'name': 'Happy'},
                {'x': sentiment_summary_filtered['date'], 'y': sentiment_summary_filtered['sad'], 'type': 'line', 'name': 'Sad'},
                {'x': sentiment_summary_filtered['date'], 'y': sentiment_summary_filtered['angry'], 'type': 'line', 'name': 'Angry'},
                {'x': sentiment_summary_filtered['date'], 'y': sentiment_summary_filtered['hope'], 'type': 'line', 'name': 'Hope'}
            ],
            'layout': {
                'title': 'Sentiment Analysis by Date',
                'xaxis': {'title': 'Date'},
                'yaxis': {'title': 'Sentiment Count'}
            }
        }

    elif graph_type == 'map':
        top_n_data = map_data.iloc[start_idx:end_idx]
        figure = {
            'data': [
                go.Choropleth(
                    locations=top_n_data['Country'],
                    locationmode='country names',
                    z=top_n_data['CommentCount'],
                    text=top_n_data['Country'],
                    colorscale='Viridis',
                    colorbar={'title': 'Comments'}
                )
            ],
            'layout': {
                'title': 'Top Countries by Comment Counts',
                'geo': {
                    'showframe': False,
                    'showcoastlines': True,
                    'projection': {'type': 'equirectangular'}
                }
            }
        }
    else:
        figure = {'data': [], 'layout': {'title': 'Invalid Graph Type Selected'}}

    return figure

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)

