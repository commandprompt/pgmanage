monitoring_widgets = [{
'dbms': 'postgresql',
'plugin_name': 'postgresql',
'id': 0,
'title': 'Transaction Rate',
'type': 'timeseries',
'interval': 10,
'default': True,
'script_chart': """
result = {
    "type": "line",
    "data": None,
    "options": {
        "legend": {
            "display": False
        },
        "responsive": True,
        "title":{
            "display":False
        },
        "tooltips": {
            "mode": "index",
            "intersect": False
        },
        "hover": {
            "mode": "nearest",
            "intersect": True
        },
        "scales": {
            "xAxes": [{
                "display": True,
                "scaleLabel": {
                    "display": False,
                    "labelString": "Time"
                }
            }],
            "yAxes": [{
                "display": True,
                "scaleLabel": {
                    "display": True,
                    "labelString": "TPS"
                },
                "ticks": {
                    "beginAtZero": True
                }
            }]
        }
    }
}
""",
'script_data': """

from datetime import datetime
from random import randint

if previous_data != None:
    query = "select round((sum(xact_commit+xact_rollback) - " + previous_data["current_count"] + ")/(extract(epoch from now()::time - '" + previous_data["current_time"] + "'::time))::numeric,2) as tps, sum(xact_commit+xact_rollback) as current_count, now()::time as current_time FROM pg_stat_database"
else:
    query = 'select 0 as tps, sum(xact_commit+xact_rollback) as current_count, now()::time as current_time FROM pg_stat_database'

query_data = connection.Query(query)

datasets = []
datasets.append({
        "label": 'Rate',
        "backgroundColor": 'rgba(129,223,129,0.4)',
        "borderColor": 'rgba(129,223,129,1)',
        "lineTension": 0,
        "pointRadius": 0,
        "borderWidth": 1,
        "data": [query_data.Rows[0]['tps']]
    })

result = {
    "labels": [datetime.now().strftime('%H:%M:%S')],
    "datasets": datasets,
    "current_count": query_data.Rows[0]['current_count'],
    'current_time': query_data.Rows[0]['current_time']
}
"""
},
{
'dbms': 'postgresql',
'plugin_name': 'postgresql',
'id': 1,
'title': 'Backends',
'type': 'timeseries',
'interval': 10,
'default': True,
'script_chart': """
max_connections = connection.ExecuteScalar('SHOW max_connections')

result = {
    "type": "line",
    "data": None,
    "options": {
        "responsive": True,
        "title":{
            "display":True,
            "text":"Backends (max_connections: " + str(max_connections) + ")"
        },
        "legend": {
            "display": False
        },
        "tooltips": {
            "mode": "index",
            "intersect": False
        },
        "hover": {
            "mode": "nearest",
            "intersect": True
        },
        "scales": {
            "xAxes": [{
                "display": True,
                "scaleLabel": {
                    "display": False,
                    "labelString": "Time"
                }
            }],
            "yAxes": [{
                "display": True,
                "scaleLabel": {
                    "display": True,
                    "labelString": "Value"
                },
                "ticks": {
                    "beginAtZero": True,
                    "max": int(max_connections)
                }
            }]
        }
    }
}
""",
'script_data': """
from datetime import datetime
from random import randint

backends = connection.Query('''
SELECT count(*) as count
FROM pg_stat_activity
''')

datasets = []
datasets.append({
        "label": 'Backends',
        "backgroundColor": 'rgba(129,223,129,0.4)',
        "borderColor": 'rgba(129,223,129,1)',
        "lineTension": 0,
        "pointRadius": 0,
        "borderWidth": 1,
        "data": [backends.Rows[0]["count"]]
    })

result = {
    "labels": [datetime.now().strftime('%H:%M:%S')],
    "datasets": datasets
}
"""
},
{
'dbms': 'postgresql',
'plugin_name': 'postgresql',
'id': 2,
'title': 'Autovacuum Workers Usage',
'type': 'timeseries',
'interval': 10,
'default': True,
'script_chart': """
result = {
    "type": "line",
    "data": None,
    "options": {
        "legend": {
            "display": False
        },
        "responsive": True,
        "title":{
            "display":False
        },
        "tooltips": {
            "mode": "index",
            "intersect": False
        },
        "hover": {
            "mode": "nearest",
            "intersect": True
        },
        "scales": {
            "xAxes": [{
                "display": True,
                "scaleLabel": {
                    "display": False,
                    "labelString": "Time"
                }
            }],
            "yAxes": [{
                "display": True,
                "scaleLabel": {
                    "display": True,
                    "labelString": "%"
                },
                "ticks": {
                    "beginAtZero": True,
                    "max": 100.0
                }
            }]
        }
    }
}
""",
'script_data': """
from datetime import datetime
from random import randint

query_data = connection.Query('''
SELECT current_setting('autovacuum_max_workers')::bigint - (SELECT count(*) FROM pg_stat_activity WHERE query LIKE 'autovacuum: %') free,
(SELECT count(*) FROM pg_stat_activity WHERE query LIKE 'autovacuum: %') used,
current_setting('autovacuum_max_workers')::bigint total
''')

perc = round((float(query_data.Rows[0]['used']))/(float(query_data.Rows[0]['total']))*100,1)

datasets = []
datasets.append({
        "label": 'Workers busy (%)',
        "backgroundColor": 'rgba(129,223,129,0.4)',
        "borderColor": 'rgba(129,223,129,1)',
        "lineTension": 0,
        "pointRadius": 0,
        "borderWidth": 1,
        "data": [perc]
    })

result = {
    "labels": [datetime.now().strftime('%H:%M:%S')],
    "datasets": datasets
}
"""
},
{
'dbms': 'postgresql',
'plugin_name': 'postgresql',
'id': 3,
'title': 'WAL Production Rate',
'type': 'timeseries',
'interval': 10,
'default': True,
'script_chart': """
result = {
    "type": "line",
    "data": None,
    "options": {
        "legend": {
            "display": False
        },
        "responsive": True,
        "title":{
            "display":False
        },
        "tooltips": {
            "mode": "index",
            "intersect": False
        },
        "hover": {
            "mode": "nearest",
            "intersect": True
        },
        "scales": {
            "xAxes": [{
                "display": True,
                "scaleLabel": {
                    "display": False,
                    "labelString": "Time"
                }
            }],
            "yAxes": [{
                "display": True,
                "scaleLabel": {
                    "display": True,
                    "labelString": "MB/s"
                },
                "ticks": {
                    "beginAtZero": True
                }
            }]
        }
    }
}
""",
'script_data': """
from datetime import datetime
from random import randint

version = int(connection.Query('show server_version_num').Rows[0][0])

if version < 100000:
    if previous_data == None:
        r = connection.Query(\"\"\"
        SELECT 0 as rate,
               current_lsn,
               current_time::text
        FROM (
        SELECT CASE WHEN pg_is_in_recovery() THEN null
               ELSE pg_current_xlog_location()
               END as current_lsn,
               now()::text as current_time) t
        \"\"\")
    else:
        r = connection.Query(\"\"\"
        SELECT round((pg_xlog_location_diff(current_lsn,'\"\"\" + previous_data["current_lsn"] + \"\"\"')/1048576.0)/(extract(epoch from now()::time - '\"\"\" + previous_data["current_time"] + \"\"\"'::time))::numeric,2) as rate,
               current_lsn,
               current_time::text
        FROM (
        SELECT CASE WHEN pg_is_in_recovery() THEN null
               ELSE pg_current_xlog_location()
               END as current_lsn,
               now() as current_time) t
        \"\"\")
else:
    if previous_data == None:
        r = connection.Query(\"\"\"
        SELECT 0 as rate,
               current_lsn,
               current_time::text
        FROM (
        SELECT CASE WHEN pg_is_in_recovery() THEN null
               ELSE pg_current_wal_lsn()
               END as current_lsn,
               now() as current_time) t
        \"\"\")
    else:
        r = connection.Query(\"\"\"
        SELECT round((pg_wal_lsn_diff(current_lsn,'\"\"\" + previous_data["current_lsn"] + \"\"\"')/1048576.0)/(extract(epoch from now()::time - '\"\"\" + previous_data["current_time"] + \"\"\"'::time))::numeric,2) as rate,
               current_lsn,
               current_time::text
        FROM (
        SELECT CASE WHEN pg_is_in_recovery() THEN null
               ELSE pg_current_wal_lsn()
               END as current_lsn,
               now() as current_time) t
        \"\"\")

datasets = []
datasets.append({
        "label": 'Rate (MB/s)',
        "backgroundColor": 'rgba(129,223,129,0.4)',
        "borderColor": 'rgba(129,223,129,1)',
        "lineTension": 0,
        "pointRadius": 0,
        "borderWidth": 1,
        "data": [r.Rows[0]['rate']]
    })

result = {
    "labels": [datetime.now().strftime('%H:%M:%S')],
    "datasets": datasets,
    "current_lsn": r.Rows[0]['current_lsn'],
    'current_time': r.Rows[0]['current_time']
}
"""
},
{
'dbms': 'postgresql',
'plugin_name': 'postgresql',
'id': 4,
'title': 'Temp Files Creation Rate',
'type': 'timeseries',
'interval': 10,
'default': True,
'script_chart': """
result = {
    "type": "line",
    "data": None,
    "options": {
        "legend": {
            "display": False
        },
        "responsive": True,
        "title":{
            "display":False
        },
        "tooltips": {
            "mode": "index",
            "intersect": False
        },
        "hover": {
            "mode": "nearest",
            "intersect": True
        },
        "scales": {
            "xAxes": [{
                "display": True,
                "scaleLabel": {
                    "display": False,
                    "labelString": "Time"
                }
            }],
            "yAxes": [{
                "display": True,
                "scaleLabel": {
                    "display": True,
                    "labelString": "MB/s"
                },
                "ticks": {
                    "beginAtZero": True
                }
            }]
        }
    }
}
""",
'script_data': """
from datetime import datetime
from random import randint

if previous_data == None:
    r = connection.Query(\"\"\"
    SELECT 0 as rate,
           sum(temp_bytes) current_temp_bytes,
           now()::text as current_time
    FROM pg_stat_database
    \"\"\")
else:
    r = connection.Query(\"\"\"
    SELECT round(((sum(temp_bytes) - \"\"\" + previous_data["current_temp_bytes"] + \"\"\")/1048576.0)/(extract(epoch from now()::time - '\"\"\" + previous_data["current_time"] + \"\"\"'::time))::numeric,2) as rate,
           sum(temp_bytes) current_temp_bytes,
           now()::text as current_time
    FROM pg_stat_database
    \"\"\")

datasets = []
datasets.append({
        "label": 'Rate (MB/s)',
        "backgroundColor": 'rgba(129,223,129,0.4)',
        "borderColor": 'rgba(129,223,129,1)',
        "lineTension": 0,
        "pointRadius": 0,
        "borderWidth": 1,
        "data": [r.Rows[0]['rate']]
    })

result = {
    "labels": [datetime.now().strftime('%H:%M:%S')],
    "datasets": datasets,
    "current_temp_bytes": r.Rows[0]['current_temp_bytes'],
    'current_time': r.Rows[0]['current_time']
}
"""
},
{
'dbms': 'postgresql',
'plugin_name': 'postgresql',
'id': 5,
'title': 'Autovacuum Freeze',
'type': 'timeseries',
'interval': 10,
'default': True,
'script_chart': """
max_age = connection.ExecuteScalar('SHOW autovacuum_freeze_max_age')

result = {
    "type": "line",
    "data": None,
    "options": {
        "legend": {
            "display": False
        },
        "responsive": True,
        "title":{
            "display":True,
            "text":"Autovacuum Freeze (autovacuum_freeze_max_age: " + str(max_age) + ")"
        },
        "tooltips": {
            "mode": "index",
            "intersect": False
        },
        "hover": {
            "mode": "nearest",
            "intersect": True
        },
        "scales": {
            "xAxes": [{
                "display": True,
                "scaleLabel": {
                    "display": True,
                    "labelString": "Time"
                }
            }],
            "yAxes": [{
                "display": True,
                "scaleLabel": {
                    "display": True,
                    "labelString": "%"
                },
                "ticks": {
                    "beginAtZero": True,
                    "max": 100.0
                }
            }]
        }
    }
}
""",
'script_data': """
from datetime import datetime
from random import randint

r = connection.Query('''
    SELECT round(max(t.perc::numeric),2) as perc
    FROM (
    SELECT (greatest(age(c.relfrozenxid), age(t.relfrozenxid))::INT8 / current_setting('autovacuum_freeze_max_age')::FLOAT)*100 as perc
    FROM (pg_class c
          JOIN pg_namespace n ON (c.relnamespace=n.oid))
    LEFT JOIN pg_class t ON c.reltoastrelid = t.oid
    WHERE c.relkind = 'r') t
''')

datasets = []
datasets.append({
        "label": 'Freeze (%)',
        "backgroundColor": 'rgba(129,223,129,0.4)',
        "borderColor": 'rgba(129,223,129,1)',
        "lineTension": 0,
        "pointRadius": 0,
        "borderWidth": 1,
        "data": [r.Rows[0]['perc']]
    })

result = {
    "labels": [datetime.now().strftime('%H:%M:%S')],
    "datasets": datasets
}
"""
},
{
'dbms': 'postgresql',
'plugin_name': 'postgresql',
'id': 6,
'title': 'Blocked Locks',
'type': 'timeseries',
'interval': 10,
'default': True,
'script_chart': """
result = {
    "type": "line",
    "data": None,
    "options": {
        "responsive": True,
        "title":{
            "display":True,
            "text":"Locks Blocked"
        },
        "legend": {
            "display": False
        },
        "tooltips": {
            "mode": "index",
            "intersect": False
        },
        "hover": {
            "mode": "nearest",
            "intersect": True
        },
        "scales": {
            "xAxes": [{
                "display": True,
                "scaleLabel": {
                    "display": False,
                    "labelString": "Time"
                }
            }],
            "yAxes": [{
                "display": True,
                "scaleLabel": {
                    "display": True,
                    "labelString": "Value"
                },
                "ticks": {
                    "beginAtZero": True
                }
            }]
        }
    }
}
""",
'script_data': """
from datetime import datetime

query_data = connection.Query('''
    SELECT count(*)
    FROM  pg_catalog.pg_locks blocked_locks
    WHERE NOT blocked_locks.GRANTED;
''')

datasets = []
datasets.append({
        "label": 'Locks Blocked',
        "backgroundColor": 'rgba(129,223,129,0.4)',
        "borderColor": 'rgba(129,223,129,1)',
        "lineTension": 0,
        "pointRadius": 0,
        "borderWidth": 1,
        "data": [query_data.Rows[0]["count"]]
    })

result = {
    "labels": [datetime.now().strftime('%H:%M:%S')],
    "datasets": datasets
}
"""
},
{
'dbms': 'postgresql',
'plugin_name': 'postgresql',
'id': 7,
'title': 'Database Size',
'type': 'timeseries',
'interval': 10,
'default': True,
'script_chart': """
result = {
    "type": "line",
    "data": None,
    "options": {
        "responsive": True,
        "title":{
            "display":True,
            "text":"Database Size"
        },
        "legend": {
            "display": False
        },
        "tooltips": {
            "mode": "index",
            "intersect": False
        },
        "hover": {
            "mode": "nearest",
            "intersect": True
        },
        "scales": {
            "xAxes": [{
                "display": True,
                "scaleLabel": {
                    "display": False,
                    "labelString": "Time"
                }
            }],
            "yAxes": [{
                "display": True,
                "scaleLabel": {
                    "display": True,
                    "labelString": "Size (MB)"
                },
                "ticks": {
                    "beginAtZero": True
                }
            }]
        }
    }
}
""",
'script_data': """
from datetime import datetime
from decimal import Decimal

query_data = connection.Query('''
    SELECT sum(pg_database_size(datname)) AS sum
    FROM pg_stat_database
    WHERE datname IS NOT NULL
''')

datasets = []
datasets.append({
        "label": 'Database Size',
        "backgroundColor": 'rgba(129,223,129,0.4)',
        "borderColor": 'rgba(129,223,129,1)',
        "lineTension": 0,
        "pointRadius": 0,
        "borderWidth": 1,
        "data": [round(query_data.Rows[0]["sum"] / Decimal(1048576.0),1)]
    })

result = {
    "labels": [datetime.now().strftime('%H:%M:%S')],
    "datasets": datasets
}
"""
}, {
'dbms': 'postgresql',
'plugin_name': 'postgresql',
'id': 8,
'title': 'Database Growth Rate',
'type': 'timeseries',
'interval': 10,
'default': True,
'script_chart': """
result = {
    "type": "line",
    "data": None,
    "options": {
        "legend": {
            "display": False
        },
        "responsive": True,
        "title":{
            "display":True,
            "text": "Database Growth Rate"
        },
        "tooltips": {
            "mode": "index",
            "intersect": False
        },
        "hover": {
            "mode": "nearest",
            "intersect": True
        },
        "scales": {
            "xAxes": [{
                "display": True,
                "scaleLabel": {
                    "display": False,
                    "labelString": "Time"
                }
            }],
            "yAxes": [{
                "display": True,
                "scaleLabel": {
                    "display": True,
                    "labelString": "MB/s"
                },
                "ticks": {
                    "beginAtZero": True
                }
            }]
        }
    }
}
""",
'script_data': """

from datetime import datetime

if previous_data != None:
    query = '''
        SELECT round(
                   ((sum(pg_database_size(datname)) - {0})/1048576.0) / (extract(epoch from now()::time - '{1}'::time))::numeric,
                   2
               ) AS database_growth,
               sum(pg_database_size(datname)) AS current_sum,
               now()::text AS current_time
        FROM pg_stat_database
        WHERE datname IS NOT NULL
    '''.format(
        previous_data['current_sum'],
        previous_data['current_time']
    )
else:
    query = '''
        SELECT 0 AS database_growth,
               sum(pg_database_size(datname)) AS current_sum,
               now()::text AS current_time
        FROM pg_stat_database
        WHERE datname IS NOT NULL
    '''

query_data = connection.Query(query)

datasets = []
datasets.append({
        "label": 'Rate',
        "backgroundColor": 'rgba(129,223,129,0.4)',
        "borderColor": 'rgba(129,223,129,1)',
        "lineTension": 0,
        "pointRadius": 0,
        "borderWidth": 1,
        "data": [query_data.Rows[0]['database_growth']]
    })

result = {
    "labels": [datetime.now().strftime('%H:%M:%S')],
    "datasets": datasets,
    "current_sum": query_data.Rows[0]['current_sum'],
    'current_time': query_data.Rows[0]['current_time']
}
"""
},
{
'dbms': 'postgresql',
'plugin_name': 'postgresql',
'id': 9,
'title': 'Heap Cache Miss Ratio',
'type': 'timeseries',
'interval': 10,
'default': True,
'script_chart': """
database_name = connection.ExecuteScalar('SELECT current_database()')

result = {
    "type": "line",
    "data": None,
    "options": {
        "responsive": True,
        "title":{
            "display":True,
            "text":"Heap Cache Miss Ratio (Database: {0})".format(database_name)
        },
        "legend": {
            "display": False
        },
        "tooltips": {
            "mode": "index",
            "intersect": False
        },
        "hover": {
            "mode": "nearest",
            "intersect": True
        },
        "scales": {
            "xAxes": [{
                "display": True,
                "scaleLabel": {
                    "display": False,
                    "labelString": "Time"
                }
            }],
            "yAxes": [{
                "display": True,
                "scaleLabel": {
                    "display": True,
                    "labelString": "%"
                },
                "ticks": {
                    "beginAtZero": True,
                    "max": 100.0
                }
            }]
        }
    }
}
""",
'script_data': """
from datetime import datetime

if previous_data != None:
    query = '''
        SELECT sum(heap_blks_read) AS current_reads,
               sum(heap_blks_hit) AS current_hits,
               now()::time AS current_time,
               CASE (sum(heap_blks_read) + sum(heap_blks_hit) - {1})
               WHEN 0 THEN 0.0
               ELSE
               round(
                   ((sum(heap_blks_read) - {0})*100::float / (sum(heap_blks_read) + sum(heap_blks_hit) - {1}))::numeric,
                   2
               ) END AS miss_ratio
        FROM pg_statio_all_tables
    '''.format(
        previous_data['current_reads'],
        int(previous_data['current_hits']) + int(previous_data['current_reads'])
    )
else:
    query = '''
        SELECT sum(heap_blks_read) AS current_reads,
               sum(heap_blks_hit) AS current_hits,
               now()::time AS current_time,
               0.0 AS miss_ratio
        FROM pg_statio_all_tables
    '''

query_data = connection.Query(query)

datasets = []
datasets.append({
        "label": 'Miss Ratio',
        "backgroundColor": 'rgba(129,223,129,0.4)',
        "borderColor": 'rgba(129,223,129,1)',
        "lineTension": 0,
        "pointRadius": 0,
        "borderWidth": 1,
        "data": [query_data.Rows[0]["miss_ratio"]]
    })

result = {
    "labels": [datetime.now().strftime('%H:%M:%S')],
    "datasets": datasets,
    "current_reads": query_data.Rows[0]['current_reads'],
    "current_hits": query_data.Rows[0]['current_hits'],
    'current_time': query_data.Rows[0]['current_time']
}
"""
},
{
'dbms': 'postgresql',
'plugin_name': 'postgresql',
'id': 10,
'title': 'Index Cache Miss Ratio',
'type': 'timeseries',
'interval': 10,
'default': True,
'script_chart': """
database_name = connection.ExecuteScalar('SELECT current_database()')

result = {
    "type": "line",
    "data": None,
    "options": {
        "responsive": True,
        "title":{
            "display":True,
            "text":"Index Cache Miss Ratio (Database: {0})".format(database_name)
        },
        "legend": {
            "display": False
        },
        "tooltips": {
            "mode": "index",
            "intersect": False
        },
        "hover": {
            "mode": "nearest",
            "intersect": True
        },
        "scales": {
            "xAxes": [{
                "display": True,
                "scaleLabel": {
                    "display": False,
                    "labelString": "Time"
                }
            }],
            "yAxes": [{
                "display": True,
                "scaleLabel": {
                    "display": True,
                    "labelString": "%"
                },
                "ticks": {
                    "beginAtZero": True,
                    "max": 100.0
                }
            }]
        }
    }
}
""",
'script_data': """
from datetime import datetime

if previous_data != None:
    query = '''
        SELECT sum(idx_blks_read) AS current_reads,
               sum(idx_blks_hit) AS current_hits,
               now()::time AS current_time,
               CASE (sum(idx_blks_read) + sum(idx_blks_hit) - {1})
               WHEN 0 THEN 0.0
               ELSE
               round(
                   ((sum(idx_blks_read) - {0})*100::float / (sum(idx_blks_read) + sum(idx_blks_hit) - {1}))::numeric,
                   2
               ) END AS miss_ratio
        FROM pg_statio_all_tables
    '''.format(
        previous_data['current_reads'],
        int(previous_data['current_hits']) + int(previous_data['current_reads'])
    )
else:
    query = '''
        SELECT sum(idx_blks_read) AS current_reads,
               sum(idx_blks_hit) AS current_hits,
               now()::time AS current_time,
               0.0 AS miss_ratio
        FROM pg_statio_all_tables
    '''

query_data = connection.Query(query)

datasets = []
datasets.append({
        "label": 'Miss Ratio',
        "backgroundColor": 'rgba(129,223,129,0.4)',
        "borderColor": 'rgba(129,223,129,1)',
        "lineTension": 0,
        "pointRadius": 0,
        "borderWidth": 1,
        "data": [query_data.Rows[0]["miss_ratio"]]
    })

result = {
    "labels": [datetime.now().strftime('%H:%M:%S')],
    "datasets": datasets,
    "current_reads": query_data.Rows[0]['current_reads'],
    "current_hits": query_data.Rows[0]['current_hits'],
    'current_time': query_data.Rows[0]['current_time']
}
"""
},
{
'dbms': 'postgresql',
'plugin_name': 'postgresql',
'id': 11,
'title': 'Seq Scan Ratio',
'type': 'timeseries',
'interval': 10,
'default': True,
'script_chart': """
database_name = connection.ExecuteScalar('SELECT current_database()')

result = {
    "type": "line",
    "data": None,
    "options": {
        "responsive": True,
        "title":{
            "display":True,
            "text":"Seq Scan Ratio (Database: {0})".format(database_name)
        },
        "legend": {
            "display": False
        },
        "tooltips": {
            "mode": "index",
            "intersect": False
        },
        "hover": {
            "mode": "nearest",
            "intersect": True
        },
        "scales": {
            "xAxes": [{
                "display": True,
                "scaleLabel": {
                    "display": False,
                    "labelString": "Time"
                }
            }],
            "yAxes": [{
                "display": True,
                "scaleLabel": {
                    "display": True,
                    "labelString": "%"
                },
                "ticks": {
                    "beginAtZero": True,
                    "max": 100.0
                }
            }]
        }
    }
}
""",
'script_data': """
from datetime import datetime

if previous_data != None:
    query = '''
        SELECT sum(seq_scan) as current_seq,
               sum(idx_scan) as current_idx,
               now()::time AS current_time,
               CASE (sum(seq_scan) + sum(idx_scan) - {1})
               WHEN 0 THEN 0.0
               ELSE
               round(
                   ((sum(seq_scan) - {0})*100::float / (sum(seq_scan) + sum(idx_scan) - {1}))::numeric,
                   2
               ) END AS ratio
        FROM pg_stat_all_tables
    '''.format(
        previous_data['current_seq'],
        int(previous_data['current_seq']) + int(previous_data['current_idx'])
    )
else:
    query = '''
        SELECT sum(seq_scan) as current_seq,
               sum(idx_scan) as current_idx,
               now()::time AS current_time,
               0.0 AS ratio
        FROM pg_stat_all_tables
    '''

query_data = connection.Query(query)

datasets = []
datasets.append({
        "label": 'Seq Scan Ratio',
        "backgroundColor": 'rgba(129,223,129,0.4)',
        "borderColor": 'rgba(129,223,129,1)',
        "lineTension": 0,
        "pointRadius": 0,
        "borderWidth": 1,
        "data": [query_data.Rows[0]["ratio"]]
    })

result = {
    "labels": [datetime.now().strftime('%H:%M:%S')],
    "datasets": datasets,
    "current_seq": query_data.Rows[0]['current_seq'],
    "current_idx": query_data.Rows[0]['current_idx'],
    'current_time': query_data.Rows[0]['current_time']
}
"""
}, {
'dbms': 'postgresql',
'plugin_name': 'postgresql',
'id': 12,
'title': 'Long Transaction',
'type': 'timeseries',
'interval': 10,
'default': True,
'script_chart': """
result = {
    "type": "line",
    "data": None,
    "options": {
        "legend": {
            "display": False
        },
        "responsive": True,
        "title":{
            "display":True,
            "text": "Long Transaction"
        },
        "tooltips": {
            "mode": "index",
            "intersect": False
        },
        "hover": {
            "mode": "nearest",
            "intersect": True
        },
        "scales": {
            "xAxes": [{
                "display": True,
                "scaleLabel": {
                    "display": False,
                    "labelString": "Time"
                }
            }],
            "yAxes": [{
                "display": True,
                "scaleLabel": {
                    "display": True,
                    "labelString": "Seconds"
                },
                "ticks": {
                    "beginAtZero": True
                }
            }]
        }
    }
}
""",
'script_data': """

from datetime import datetime

if int(connection.ExecuteScalar('show server_version_num')) < 100000:
    query = '''
        SELECT seconds
        FROM (
            SELECT ROUND(EXTRACT(EPOCH FROM (clock_timestamp()-xact_start))::numeric,2) as seconds
            FROM pg_stat_activity
            WHERE xact_start is not null
              AND datid is not null
              AND query NOT LIKE 'autovacuum: %'
        ) x
        ORDER BY seconds DESC
        LIMIT 1
    '''
else:
    query = '''
        SELECT seconds
        FROM (
            SELECT ROUND(EXTRACT(EPOCH FROM (clock_timestamp()-xact_start))::numeric,2) as seconds
            FROM pg_stat_activity
            WHERE xact_start is not null
              AND datid is not null
              AND backend_type NOT IN ('walreceiver','walsender','walwriter','autovacuum worker')
        ) x
        ORDER BY seconds DESC
        LIMIT 1
    '''

query_data = connection.Query(query)

datasets = []
datasets.append({
        "label": 'Seconds',
        "backgroundColor": 'rgba(129,223,129,0.4)',
        "borderColor": 'rgba(129,223,129,1)',
        "lineTension": 0,
        "pointRadius": 0,
        "borderWidth": 1,
        "data": [query_data.Rows[0]['seconds']]
    })

result = {
    "labels": [datetime.now().strftime('%H:%M:%S')],
    "datasets": datasets
}
"""
}, {
'dbms': 'postgresql',
'plugin_name': 'postgresql',
'id': 13,
'title': 'Long Query',
'type': 'timeseries',
'interval': 10,
'default': True,
'script_chart': """
result = {
    "type": "line",
    "data": None,
    "options": {
        "legend": {
            "display": False
        },
        "responsive": True,
        "title":{
            "display":True,
            "text": "Long Query"
        },
        "tooltips": {
            "mode": "index",
            "intersect": False
        },
        "hover": {
            "mode": "nearest",
            "intersect": True
        },
        "scales": {
            "xAxes": [{
                "display": True,
                "scaleLabel": {
                    "display": False,
                    "labelString": "Time"
                }
            }],
            "yAxes": [{
                "display": True,
                "scaleLabel": {
                    "display": True,
                    "labelString": "Seconds"
                },
                "ticks": {
                    "beginAtZero": True
                }
            }]
        }
    }
}
""",
'script_data': """

from datetime import datetime

if int(connection.ExecuteScalar('show server_version_num')) < 100000:
    query = '''
        SELECT seconds
            FROM (
                SELECT ROUND(EXTRACT(EPOCH FROM (clock_timestamp()-query_start))::numeric,2) as seconds
                FROM pg_stat_activity
                WHERE state='active'
                  AND query_start is not null
                  AND datid is not null
                  AND query NOT LIKE 'autovacuum: %'
                UNION ALL
                SELECT 0.0
            ) x
            ORDER BY seconds DESC
            LIMIT 1
    '''
else:
    query = '''
        SELECT seconds
        FROM (
            SELECT ROUND(EXTRACT(EPOCH FROM (clock_timestamp()-query_start))::numeric,2) as seconds
            FROM pg_stat_activity
            WHERE state='active'
              AND query_start is not null
              AND datid is not null
              AND backend_type NOT IN ('walreceiver','walsender','walwriter','autovacuum worker')
            UNION ALL
            SELECT 0.0
        ) x
        ORDER BY seconds DESC
        LIMIT 1
    '''

query_data = connection.Query(query)

datasets = []
datasets.append({
        "label": 'Seconds',
        "backgroundColor": 'rgba(129,223,129,0.4)',
        "borderColor": 'rgba(129,223,129,1)',
        "lineTension": 0,
        "pointRadius": 0,
        "borderWidth": 1,
        "data": [query_data.Rows[0]['seconds']]
    })

result = {
    "labels": [datetime.now().strftime('%H:%M:%S')],
    "datasets": datasets
}
"""
}, {
'dbms': 'postgresql',
'plugin_name': 'postgresql',
'id': 14,
'title': 'Long Autovacuum',
'type': 'timeseries',
'interval': 10,
'default': True,
'script_chart': """
result = {
    "type": "line",
    "data": None,
    "options": {
        "legend": {
            "display": False
        },
        "responsive": True,
        "title":{
            "display":True,
            "text": "Long Autovacuum"
        },
        "tooltips": {
            "mode": "index",
            "intersect": False
        },
        "hover": {
            "mode": "nearest",
            "intersect": True
        },
        "scales": {
            "xAxes": [{
                "display": True,
                "scaleLabel": {
                    "display": False,
                    "labelString": "Time"
                }
            }],
            "yAxes": [{
                "display": True,
                "scaleLabel": {
                    "display": True,
                    "labelString": "Seconds"
                },
                "ticks": {
                    "beginAtZero": True
                }
            }]
        }
    }
}
""",
'script_data': """

from datetime import datetime

if int(connection.ExecuteScalar('show server_version_num')) < 100000:
    query = '''
        SELECT seconds
        FROM (
            SELECT ROUND(EXTRACT(EPOCH FROM (clock_timestamp()-query_start))::numeric,2) as seconds
            FROM pg_stat_activity
            WHERE state='active'
              AND query_start is not null
              AND datid is not null
              AND query LIKE 'autovacuum: %'
            UNION ALL
            SELECT 0.0
        ) x
        ORDER BY seconds DESC
        LIMIT 1
    '''
else:
    query = '''
        SELECT seconds
        FROM (
            SELECT ROUND(EXTRACT(EPOCH FROM (clock_timestamp()-query_start))::numeric,2) as seconds
            FROM pg_stat_activity
            WHERE state='active'
              AND query_start is not null
              AND datid is not null
              AND backend_type = 'autovacuum worker'
            UNION ALL
            SELECT 0.0
        ) x
        ORDER BY seconds DESC
        LIMIT 1
    '''

query_data = connection.Query(query)

datasets = []
datasets.append({
        "label": 'Seconds',
        "backgroundColor": 'rgba(129,223,129,0.4)',
        "borderColor": 'rgba(129,223,129,1)',
        "lineTension": 0,
        "pointRadius": 0,
        "borderWidth": 1,
        "data": [query_data.Rows[0]['seconds']]
    })

result = {
    "labels": [datetime.now().strftime('%H:%M:%S')],
    "datasets": datasets
}
"""
}, {
'dbms': 'postgresql',
'plugin_name': 'postgresql',
'id': 15,
'title': 'Checkpoints',
'type': 'timeseries',
'interval': 10,
'default': True,
'script_chart': """
result = {
    "type": "line",
    "data": None,
    "options": {
        "legend": {
            "display": False
        },
        "responsive": True,
        "title":{
            "display":True,
            "text": "Checkpoints"
        },
        "tooltips": {
            "mode": "index",
            "intersect": False
        },
        "hover": {
            "mode": "nearest",
            "intersect": True
        },
        "scales": {
            "xAxes": [{
                "display": True,
                "scaleLabel": {
                    "display": False,
                    "labelString": "Time"
                }
            }],
            "yAxes": [{
                "display": True,
                "scaleLabel": {
                    "display": True,
                    "labelString": "Checkpoints"
                },
                "ticks": {
                    "beginAtZero": True
                }
            }]
        }
    }
}
""",
'script_data': """

from datetime import datetime

if previous_data != None:
    query = "select (checkpoints_timed+checkpoints_req) - " + str(previous_data["current_checkpoints"]) + " as checkpoints_diff, (checkpoints_timed+checkpoints_req) as current_checkpoints FROM pg_stat_bgwriter"
else:
    query = 'select 0 as checkpoints_diff, (checkpoints_timed+checkpoints_req) as current_checkpoints FROM pg_stat_bgwriter'

query_data = connection.Query(query)

datasets = []
datasets.append({
        "label": 'Checkpoints',
        "backgroundColor": 'rgba(129,223,129,0.4)',
        "borderColor": 'rgba(129,223,129,1)',
        "lineTension": 0,
        "pointRadius": 0,
        "borderWidth": 1,
        "data": [query_data.Rows[0]['checkpoints_diff']]
    })

result = {
    "labels": [datetime.now().strftime('%H:%M:%S')],
    "datasets": datasets,
    "current_checkpoints": query_data.Rows[0]['current_checkpoints']
}
"""
},
{
'dbms': 'postgresql',
'plugin_name': 'postgresql',
'id': 16,
'title': 'Activity',
'type': 'grid',
'interval': 10,
'default': False,
'script_chart': '',
'script_data': """
from datetime import datetime

data = connection.Query('''
    SELECT *
    FROM pg_stat_activity
''')

result = {
    "columns": data.Columns,
    "data": data.Rows
}
"""
},
{
'dbms': 'postgresql',
'plugin_name': 'postgresql',
'id': 17,
'title': 'Bloat: Top 20 Tables',
'type': 'grid',
'interval': 30,
'default': True,
'script_chart': '',
'script_data': """
from datetime import datetime

data = connection.Query('''
    SELECT z.current_database,z.schemaname,z.tablename, pg_size_pretty(sum_wasted) AS total_bloat
    FROM (
    SELECT y.schemaname, y.tablename, y.current_database, sum(wastedbytes+wastedibytes)::bigint AS sum_wasted
    FROM (
    SELECT current_database,schemaname, tablename, tbloat, wastedbytes, iname, ibloat, wastedibytes AS wastedibytes
    FROM (
    SELECT
      current_database(), schemaname, tablename, /*reltuples::bigint, relpages::bigint, otta,*/
      ROUND((CASE WHEN otta=0 THEN 0.0 ELSE sml.relpages::FLOAT/otta END)::NUMERIC,1) AS tbloat,
      CASE WHEN relpages < otta THEN 0 ELSE bs*(sml.relpages-otta)::BIGINT END AS wastedbytes,
      iname, /*ituples::bigint, ipages::bigint, iotta,*/
      ROUND((CASE WHEN iotta=0 OR ipages=0 THEN 0.0 ELSE ipages::FLOAT/iotta END)::NUMERIC,1) AS ibloat,
      CASE WHEN ipages < iotta THEN 0 ELSE bs*(ipages-iotta) END AS wastedibytes
    FROM (
      SELECT
        schemaname, tablename, cc.reltuples, cc.relpages, bs,
        CEIL((cc.reltuples*((datahdr+ma-
          (CASE WHEN datahdr%ma=0 THEN ma ELSE datahdr%ma END))+nullhdr2+4))/(bs-20::FLOAT)) AS otta,
        COALESCE(c2.relname,'?') AS iname, COALESCE(c2.reltuples,0) AS ituples, COALESCE(c2.relpages,0) AS ipages,
        COALESCE(CEIL((c2.reltuples*(datahdr-12))/(bs-20::FLOAT)),0) AS iotta -- very rough approximation, assumes all cols
      FROM (
        SELECT
          ma,bs,schemaname,tablename,
          (datawidth+(hdr+ma-(CASE WHEN hdr%ma=0 THEN ma ELSE hdr%ma END)))::NUMERIC AS datahdr,
          (maxfracsum*(nullhdr+ma-(CASE WHEN nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2
        FROM (
          SELECT
            schemaname, tablename, hdr, ma, bs,
            SUM((1-null_frac)*avg_width) AS datawidth,
            MAX(null_frac) AS maxfracsum,
            hdr+(
              SELECT 1+COUNT(*)/8
              FROM pg_stats s2
              WHERE null_frac<>0 AND s2.schemaname = s.schemaname AND s2.tablename = s.tablename
            ) AS nullhdr
          FROM pg_stats s, (
            SELECT
              (SELECT current_setting('block_size')::NUMERIC) AS bs,
              CASE WHEN SUBSTRING(v,12,3) IN ('8.0','8.1','8.2') THEN 27 ELSE 23 END AS hdr,
              CASE WHEN v ~ 'mingw32' THEN 8 ELSE 4 END AS ma
            FROM (SELECT version() AS v) AS foo
          ) AS constants
          GROUP BY 1,2,3,4,5
        ) AS foo
      ) AS rs
      JOIN pg_class cc ON cc.relname = rs.tablename
      JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = rs.schemaname AND nn.nspname <> 'information_schema'
      LEFT JOIN pg_index i ON indrelid = cc.oid
      LEFT JOIN pg_class c2 ON c2.oid = i.indexrelid
    ) AS sml) x) y
    GROUP BY y.schemaname, y.tablename, y.current_database) z
    ORDER BY z.sum_wasted DESC
    LIMIT 20
''')

result = {
    "columns": data.Columns,
    "data": data.Rows
}
"""
},
{
'dbms': 'postgresql',
'plugin_name': 'postgresql',
'id': 18,
'title': 'In Recovery',
'type': 'grid',
'interval': 120,
'default': True,
'script_chart': '',
'script_data': """
data = connection.Query('''
    SELECT pg_is_in_recovery() as "In Recovery"
''')

result = {
    "columns": data.Columns,
    "data": data.Rows
}
"""
},
{
'dbms': 'postgresql',
'plugin_name': 'postgresql',
'id': 19,
'title': 'Autovac Freeze: Top 20 Tables',
'type': 'grid',
'interval': 60,
'default': True,
'script_chart': '',
'script_data': """
from datetime import datetime

data = connection.Query('''
    SELECT relname as table_name,
           pg_size_pretty(pg_table_size(oid)) as table_size,
           age(relfrozenxid) as xid_age,
           current_setting('autovacuum_freeze_max_age')::integer as max_age,
           round(age(relfrozenxid)/(current_setting('autovacuum_freeze_max_age')::integer)::numeric*100.0,4) as perc
    FROM pg_class
    WHERE relkind in ('r', 't')
    ORDER BY age(relfrozenxid) DESC
    LIMIT 20;
''')

result = {
    "columns": data.Columns,
    "data": data.Rows
}
"""
},
{
'dbms': 'postgresql',
'plugin_name': 'postgresql',
'id': 20,
'title': 'Database Size',
'type': 'chart',
'interval': 60,
'default': True,
'script_chart': """
total_size = connection.ExecuteScalar('''
    SELECT round(sum(pg_catalog.pg_database_size(datname)/1048576.0),2)
    FROM pg_catalog.pg_database
    WHERE NOT datistemplate
''')

result = {
    "type": "pie",
    "data": None,
    "options": {
        "legend": {
            "position": "bottom",
        },
        "responsive": True,
        "title":{
            "display":True,
            "text":"Database Size (Total: " + str(total_size) + " MB)"
        }
    }
}
""",
'script_data': """
from datetime import datetime
from random import randint

databases = connection.Query('''
    SELECT d.datname AS datname,
           round(pg_catalog.pg_database_size(d.datname)/1048576.0,2) AS size
    FROM pg_catalog.pg_database d
    WHERE d.datname not in ('template0','template1')
''')

data = []
color = []
label = []

for db in databases.Rows:
    data.append(db["size"])
    color.append("rgb(" + str(randint(125, 225)) + "," + str(randint(125, 225)) + "," + str(randint(125, 225)) + ")")
    label.append(db["datname"])

total_size = connection.ExecuteScalar('''
    SELECT round(sum(pg_catalog.pg_database_size(datname)/1048576.0),2)
    FROM pg_catalog.pg_database
    WHERE NOT datistemplate
''')

result = {
    "labels": label,
    "datasets": [
        {
            "data": data,
            "backgroundColor": color,
            "label": "Dataset 1"
        }
    ],
    "title": "Database Size (Total: " + str(total_size) + " MB)"
}
"""
},
{
'dbms': 'postgresql',
'plugin_name': 'postgresql',
'id': 21,
'title': 'Backends',
'type': 'chart',
'interval': 60,
'default': True,
'script_chart': """
max_connections = connection.ExecuteScalar('SHOW max_connections')

result = {
    "type": "pie",
    "data": None,
    "options": {
        "legend": {
            "position": "bottom",
        },
        "responsive": True,
        "title":{
            "display":True,
            "text":"Backends (max_connections: " + str(max_connections) + ")"
        }
    }
}
""",
'script_data': """
from datetime import datetime
from random import randint

databases = connection.Query('''
    SELECT d.datname,
           s.numbackends
    FROM pg_stat_database s
    INNER JOIN pg_database d
    ON d.oid = s.datid
    WHERE NOT d.datistemplate
''')

data = []
color = []
label = []

for db in databases.Rows:
    data.append(db["numbackends"])
    color.append("rgb(" + str(randint(125, 225)) + "," + str(randint(125, 225)) + "," + str(randint(125, 225)) + ")")
    label.append(db["datname"])

result = {
    "labels": label,
    "datasets": [
        {
            "data": data,
            "backgroundColor": color,
            "label": "Dataset 1"
        }
    ]
}
"""
},
]
