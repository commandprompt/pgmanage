monitoring_widgets = [{
'dbms': 'mysql',
'plugin_name': 'mysql',
'id': 0,
'title': 'Thread Count',
'type': 'timeseries',
'interval': 10,
'default': True,
'script_chart': """

max_connections = connection.Query('show variables like "max_connections"').Rows[0]['Value']

result = {
    "type": "line",
    "data": None,
    "options": {
        "plugins": {
            "legend": {
                "display": False
            },
            "title":{
                "display": True,
                "text":"Threads (max_connections: " + str(max_connections) + ")"
            },
            "tooltip": {
                "mode": "index",
                "intersect": False
            },
        },
        "responsive": True,
        "hover": {
            "mode": "nearest",
            "intersect": True
        },
        "scales": {
            "x": {
                "display": True,
                "title": {
                    "display": False,
                    "text": "Time"
                }
            },
            "y": {
                "display": True,
                "title": {
                    "display": True,
                    "text": "Value"
                },
                "beginAtZero": True,
                "max": int(max_connections)
            }
        }
    }
}
""",
'script_data': """
from datetime import datetime
from random import randint

backends = connection.Query('''
show status where `variable_name` = 'Threads_connected';
''')

datasets = []
datasets.append({
        "label": 'Backends',
        "backgroundColor": 'rgba(129,223,129,0.4)',
        "borderColor": 'rgba(129,223,129,1)',
        "lineTension": 0,
        "pointRadius": 0,
        "borderWidth": 1,
        "data": [backends.Rows[0]["Value"]]
    })

result = {
    "labels": [datetime.now().strftime('%H:%M:%S')],
    "datasets": datasets
}
"""
}]
