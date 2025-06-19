monitoring_widgets = [{
'dbms': 'mysql',
'plugin_name': 'mysql',
'id': -1,
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
                },
                "type": "time",
                "time": {
                    "unit": "minute",
                    "stepSize": 15,
                    "displayFormats": {
                        "minute": "HH:mm:ss"
                    }
                },
                "ticks": {
                    "stepSize": 0.25
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
        "fill": True,
        "tension": 0.2,
        "cubicInterpolationMode": "monotone",
        "pointRadius": 0,
        "borderWidth": 1.2,
        "data": [{
            "x": datetime.now().isoformat(),
            "y":int(backends.Rows[0]["Value"])
        }]
    })

result = {
    "datasets": datasets
}
"""
}]
